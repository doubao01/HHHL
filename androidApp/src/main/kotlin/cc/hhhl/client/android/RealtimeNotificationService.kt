package cc.hhhl.client.android

import android.app.Notification
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.graphics.BitmapFactory
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import cc.hhhl.client.automation.AutomationTrigger
import cc.hhhl.client.automation.toMainStreamingOptions
import cc.hhhl.client.api.ChatStreamingEvent
import cc.hhhl.client.api.MainStreamingEvent
import cc.hhhl.client.api.MainStreamingOptions
import cc.hhhl.client.auth.AccountSession
import cc.hhhl.client.repository.ChatStreamingRepository
import cc.hhhl.client.model.ChatMessage
import cc.hhhl.client.model.NotificationItem
import cc.hhhl.client.model.NotificationType
import cc.hhhl.client.repository.MainStreamingRepository
import java.time.Instant
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

class RealtimeNotificationService : Service() {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var lastTimelineSyncAt: Long = 0L
    private var lastRealtimeChatSyncAt: Long = 0L
    @Volatile
    private var lastRealtimeActivityAt: Long = System.currentTimeMillis()
    private var realtimeWakeLock: PowerManager.WakeLock? = null
    @Volatile
    private var shouldScheduleRecovery: Boolean = true

    override fun onCreate() {
        super.onCreate()
        ensureChannels(applicationContext)
        if (!startRealtimeForegroundSafely()) {
            BackgroundNotificationScheduler.syncSoon(applicationContext)
            stopSelf()
            return
        }
        renewRealtimeWakeLock()
        instanceRunning = true
        hibernatedDueToIdle = false
        scope.launch {
            runRealtimeLoop()
        }
        scope.launch {
            runPollingSafetyLoop()
        }
        scope.launch {
            runRealtimeChatLoop()
        }
        scope.launch {
            runIdleGovernorLoop()
        }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (!AndroidBackgroundNotificationStore(applicationContext).isBackgroundSyncEnabled()) {
            shouldScheduleRecovery = false
            stopSelf()
            return START_NOT_STICKY
        }
        return START_STICKY
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onDestroy() {
        instanceRunning = false
        scope.cancel()
        releaseRealtimeWakeLock()
        if (shouldScheduleRecovery && AndroidBackgroundNotificationStore(applicationContext).isBackgroundSyncEnabled()) {
            BackgroundNotificationScheduler.syncSoon(applicationContext)
        }
        super.onDestroy()
    }

    override fun onTaskRemoved(rootIntent: Intent?) {
        super.onTaskRemoved(rootIntent)
        BackgroundNotificationScheduler.restoreIfEnabled(applicationContext)
    }

    private suspend fun runRealtimeLoop() {
        val settings = AndroidBackgroundNotificationStore(applicationContext)
        while (settings.isBackgroundSyncEnabled()) {
            renewRealtimeWakeLock()
            val session = AndroidAuthTokenStore(applicationContext)
                .readAccountSessions()
                .firstOrNull { it.current }
            if (session == null) {
                delay(RECONNECT_DELAY_MS)
                continue
            }
            val token = session.token.trim()
            if (token.isBlank()) {
                delay(RECONNECT_DELAY_MS)
                continue
            }

            var unauthorized = false
            runCatching {
                MainStreamingRepository(tokenProvider = { token }).streamMain(mainStreamingOptions()).collect { event ->
                    if (event.isRealtimeActivitySignal()) noteRealtimeActivity()
                    when (event) {
                        MainStreamingEvent.UnreadNotification -> syncNotificationEvent()
                        is MainStreamingEvent.NotificationReceived -> syncNotificationEvent(event.notification)
                        is MainStreamingEvent.ChatMessageReceived -> {
                            val directUserId = if (event.message.roomId.isBlank()) {
                                event.message.directPeerId(session.user?.id)
                            } else {
                                null
                            }
                            val handledRealtime = if (event.message.roomId.isNotBlank() || directUserId != null) {
                                BackgroundNotificationSyncer(applicationContext).handleRealtimeChatMessage(
                                    session = session,
                                    message = event.message,
                                    directUserId = directUserId,
                                    roomId = event.message.roomId,
                                )
                            } else {
                                false
                            }
                            if (!handledRealtime) syncRealtimeChatEvent(debounce = true)
                        }
                        is MainStreamingEvent.ChatMessageDeleted -> syncRealtimeChatEvent(debounce = true)
                        MainStreamingEvent.NewChatMessage -> syncRealtimeChatEvent()
                        is MainStreamingEvent.TimelineNote -> syncTimelineEvent(event)
                        MainStreamingEvent.Unauthorized -> unauthorized = true
                        MainStreamingEvent.ReadAllNotifications,
                        MainStreamingEvent.Connected,
                        MainStreamingEvent.Connecting,
                        MainStreamingEvent.Closed,
                        is MainStreamingEvent.Error,
                        -> Unit
                    }
                }
            }
            if (unauthorized) {
                shouldScheduleRecovery = false
                stopSelf()
                return
            }
            delay(RECONNECT_DELAY_MS)
        }
        shouldScheduleRecovery = false
        stopSelf()
    }

    private suspend fun runPollingSafetyLoop() {
        val settings = AndroidBackgroundNotificationStore(applicationContext)
        val syncer = BackgroundNotificationSyncer(applicationContext)
        while (scope.isActive && settings.isBackgroundSyncEnabled()) {
            renewRealtimeWakeLock()
            val result = syncer.sync(trigger = BackgroundNotificationSyncTrigger.PollingSafety)
            if (result == BackgroundNotificationSyncResult.Retry) {
                delay(POLLING_SAFETY_INTERVAL_MS)
                continue
            }
            delay(idleSyncIntervalMs(POLLING_SAFETY_INTERVAL_MS, POLLING_SAFETY_MAX_INTERVAL_MS))
        }
    }

    private suspend fun runRealtimeChatLoop() {
        val settings = AndroidBackgroundNotificationStore(applicationContext)
        while (scope.isActive && settings.isBackgroundSyncEnabled()) {
            renewRealtimeWakeLock()
            val session = AndroidAuthTokenStore(applicationContext)
                .readAccountSessions()
                .firstOrNull { it.current }
            val token = session?.token?.trim().orEmpty()
            if (session == null || token.isBlank()) {
                delay(RECONNECT_DELAY_MS)
                continue
            }

            val targets = BackgroundNotificationSyncer(applicationContext).loadRealtimeChatStreamTargets(session)
            if (targets.isEmpty) {
                delay(idleSyncIntervalMs(CHAT_TARGET_REFRESH_INTERVAL_MS, CHAT_TARGET_REFRESH_MAX_INTERVAL_MS))
                continue
            }

            var unauthorized = false
            runCatching {
                coroutineScope {
                    val streamJob = launch {
                        ChatStreamingRepository(tokenProvider = { token }).streamMessages(
                            roomIds = targets.roomIds,
                            userIds = targets.userIds,
                        ).collect { event ->
                            if (event.isRealtimeActivitySignal()) noteRealtimeActivity()
                            when (event) {
                                is ChatStreamingEvent.MessageReceived -> {
                                    val sourceRoomId = event.source.roomId?.takeIf { roomId -> roomId in targets.roomIdSet }
                                    val sourceUserId = event.source.userId?.takeIf { userId -> userId in targets.userIdSet }
                                    val fallbackUserId = if (sourceRoomId == null) {
                                        targets.userIds.firstOrNull { userId ->
                                            event.message.belongsToDirectChat(userId) && event.message.roomId !in targets.roomIdSet
                                        }
                                    } else {
                                        null
                                    }
                                    val messageDirectUserId = event.message
                                        .directPeerId(session.user?.id)
                                        .takeIf { event.message.roomId.isBlank() }
                                    val directUserId = sourceUserId ?: fallbackUserId ?: messageDirectUserId
                                    val messageRoomId = event.message.roomId.takeIf { it.isNotBlank() }
                                    val roomId = sourceRoomId ?: messageRoomId
                                    val handledRealtime = BackgroundNotificationSyncer(applicationContext).handleRealtimeChatMessage(
                                        session = session,
                                        message = event.message,
                                        directUserId = directUserId,
                                        roomId = roomId,
                                        roomName = roomId?.let { id -> targets.roomNamesById[id] }.orEmpty(),
                                    )
                                    if (!handledRealtime) syncRealtimeChatEvent(debounce = true)
                                }
                                is ChatStreamingEvent.MessageDeleted -> syncRealtimeChatEvent(debounce = true)
                                ChatStreamingEvent.Unauthorized -> unauthorized = true
                                ChatStreamingEvent.Connecting,
                                ChatStreamingEvent.Connected,
                                ChatStreamingEvent.Closed,
                                is ChatStreamingEvent.Error,
                                -> Unit
                            }
                        }
                    }
                    val watcherJob = launch {
                        watchRealtimeChatTargets(session = session, currentTargets = targets, streamJob = streamJob)
                    }
                    streamJob.join()
                    watcherJob.cancel()
                }
            }
            if (unauthorized) {
                shouldScheduleRecovery = false
                stopSelf()
                return
            }
            delay(RECONNECT_DELAY_MS)
        }
        shouldScheduleRecovery = false
        stopSelf()
    }

    private suspend fun watchRealtimeChatTargets(
        session: AccountSession,
        currentTargets: RealtimeChatStreamTargets,
        streamJob: Job,
    ) {
        val syncer = BackgroundNotificationSyncer(applicationContext)
        var current = currentTargets
        while (streamJob.isActive && scope.isActive) {
            delay(idleSyncIntervalMs(CHAT_TARGET_REFRESH_INTERVAL_MS, CHAT_TARGET_REFRESH_MAX_INTERVAL_MS))
            if (!streamJob.isActive) break
            val next = syncer.loadRealtimeChatStreamTargets(session)
            if (next.isEmpty || next.roomIdSet != current.roomIdSet || next.userIdSet != current.userIdSet) {
                streamJob.cancel()
                break
            }
            current = next
        }
    }

    private suspend fun runIdleGovernorLoop() {
        val settings = AndroidBackgroundNotificationStore(applicationContext)
        while (scope.isActive && settings.isBackgroundSyncEnabled()) {
            if (System.currentTimeMillis() - lastRealtimeActivityAt >= IDLE_HIBERNATE_AFTER_MS) {
                hibernateForIdle()
                return
            }
            renewRealtimeWakeLock()
            delay(WAKE_LOCK_RENEW_INTERVAL_MS)
        }
    }

    private fun hibernateForIdle() {
        hibernatedDueToIdle = true
        shouldScheduleRecovery = false
        releaseRealtimeWakeLock()
        stopSelf()
    }

    private suspend fun syncNotificationEvent(notification: NotificationItem? = null) {
        noteRealtimeActivity()
        notification?.let {
            publishStreamingNotification(it)
            emitStreamingNotificationAutomation(it)
        }
        val syncer = BackgroundNotificationSyncer(applicationContext)
        syncer.sync(trigger = BackgroundNotificationSyncTrigger.RealtimeNotification)
        delay(NOTIFICATION_EVENT_RECHECK_DELAY_MS)
        syncer.sync(trigger = BackgroundNotificationSyncTrigger.RealtimeNotification)
    }

    private suspend fun syncTimelineEvent(event: MainStreamingEvent.TimelineNote) {
        noteRealtimeActivity()
        publishSpecialCareTimelineNote(event)
        if (!hasEnabledTimelineAutomationRules()) return
        val now = System.currentTimeMillis()
        if (now - lastTimelineSyncAt < TIMELINE_SYNC_DEBOUNCE_MS) return
        lastTimelineSyncAt = now
        BackgroundNotificationSyncer(applicationContext).sync(trigger = BackgroundNotificationSyncTrigger.RealtimeTimeline)
    }

    private suspend fun hasEnabledTimelineAutomationRules(): Boolean {
        val session = AndroidAuthTokenStore(applicationContext)
            .readAccountSessions()
            .firstOrNull { it.current }
            ?: return false
        return AndroidAutomationStore(applicationContext)
            .read(session.id)
            .rules
            .any { rule -> rule.enabled && rule.trigger == AutomationTrigger.TimelineNote }
    }

    private suspend fun mainStreamingOptions(): MainStreamingOptions {
        val session = AndroidAuthTokenStore(applicationContext)
            .readAccountSessions()
            .firstOrNull { it.current }
            ?: return MainStreamingOptions()
        return AndroidAutomationStore(applicationContext)
            .read(session.id)
            .rules
            .toMainStreamingOptions(MAX_STREAMING_CHANNELS)
    }

    private suspend fun emitStreamingNotificationAutomation(notification: NotificationItem) {
        if (notification.id.isBlank() || notification.isRead) return
        BackgroundNotificationSyncer(applicationContext).emitNotificationAutomation(notification)
    }

    private suspend fun syncRealtimeChatEvent(debounce: Boolean = false) {
        noteRealtimeActivity()
        if (debounce) {
            val now = System.currentTimeMillis()
            if (now - lastRealtimeChatSyncAt < CHAT_SYNC_DEBOUNCE_MS) return
            lastRealtimeChatSyncAt = now
        }
        val syncer = BackgroundNotificationSyncer(applicationContext)
        syncer.sync(trigger = BackgroundNotificationSyncTrigger.RealtimeChat)
        delay(CHAT_EVENT_RECHECK_DELAY_MS)
        syncer.sync(trigger = BackgroundNotificationSyncTrigger.RealtimeChat)
    }

    private suspend fun publishStreamingNotification(notification: NotificationItem) {
        if (notification.id.isBlank() || notification.isRead) return
        val session = AndroidAuthTokenStore(applicationContext)
            .readAccountSessions()
            .firstOrNull { it.current }
            ?: return
        val settings = AndroidBackgroundNotificationStore(applicationContext)
        val eventId = notification.notificationEventId()

        val specialCareUserIds = AndroidSpecialCareStore(applicationContext).loadSpecialCareUserIds(session.id)
        val isSpecialCare = notification.actor.id in specialCareUserIds
        if (isSpecialCare && !settings.isSpecialCareEnabled()) return
        if (!settings.claimSeenIdGroup(notification.notificationSeenIds(), MAX_REALTIME_SEEN_IDS)) return
        val createdAtEpochMillis = notification.createdAtEpochMillis.takeIf { it > 0L }
            ?: System.currentTimeMillis()
        BackgroundNotificationPublisher(applicationContext).publish(
            listOf(
                BackgroundNotificationEvent(
                    id = eventId,
                    title = if (isSpecialCare) "特别关心 · ${notification.actor.displayName}" else notification.actor.displayName,
                    text = notification.notePreviewText?.takeIf { it.isNotBlank() } ?: notification.text,
                    specialCare = isSpecialCare,
                    gatedBySpecialCareSetting = isSpecialCare,
                    avatarMode = if (notification.isAppNotification()) {
                        NotificationAvatarMode.AppIcon
                    } else if (isSpecialCare) {
                        NotificationAvatarMode.UserAvatar(notification.actor.avatarUrl, notification.actor.avatarInitial)
                    } else {
                        NotificationAvatarMode.Transparent
                    },
                    createdAtEpochMillis = createdAtEpochMillis,
                    cacheNotification = notification.takeIf { isSpecialCare }?.copy(
                        isSpecialCare = true,
                        createdAtEpochMillis = createdAtEpochMillis,
                    ),
                ),
            ),
        )
        applicationContext.cacheRemoteNotifications(
            accountId = session.id,
            notifications = listOf(notification.copy(createdAtEpochMillis = createdAtEpochMillis)),
        )
        if (isSpecialCare) {
            applicationContext.cacheSpecialCareNotifications(
                accountId = session.id,
                notifications = listOf(notification.copy(isSpecialCare = true, createdAtEpochMillis = createdAtEpochMillis)),
            )
        }
    }

    private suspend fun publishSpecialCareTimelineNote(event: MainStreamingEvent.TimelineNote): Boolean {
        val note = event.note ?: return false
        val session = AndroidAuthTokenStore(applicationContext)
            .readAccountSessions()
            .firstOrNull { it.current }
            ?: return false
        val settings = AndroidBackgroundNotificationStore(applicationContext)
        if (!settings.isSpecialCareEnabled()) return true

        val specialCareUserIds = AndroidSpecialCareStore(applicationContext).loadSpecialCareUserIds(session.id)
        if (note.author.id !in specialCareUserIds) return true

        val eventId = "special-note:${note.id}"
        val createdAtEpochMillis = note.createdAt.toEpochMillisOrNull()
            ?: System.currentTimeMillis()
        val notification = NotificationItem(
            id = "special-care-note-${note.id}",
            type = NotificationType.Note,
            actor = note.author,
            text = "发布了新帖子",
            createdAtLabel = note.createdAtLabel.ifBlank { "刚刚" },
            createdAtEpochMillis = createdAtEpochMillis,
            noteId = note.id,
            notePreviewText = note.text.takeIf { it.isNotBlank() } ?: note.cw,
            isSpecialCare = true,
        )
        if (!settings.claimSeenIdGroup(notification.notificationSeenIds(), MAX_REALTIME_SEEN_IDS)) return true
        BackgroundNotificationPublisher(applicationContext).publish(
            listOf(
                BackgroundNotificationEvent(
                    id = eventId,
                    title = "特别关心 · ${note.author.displayName}",
                    text = note.text.ifBlank { "发布了新帖子" },
                    specialCare = true,
                    avatarMode = NotificationAvatarMode.UserAvatar(
                        note.author.avatarUrl,
                        note.author.avatarInitial,
                    ),
                    createdAtEpochMillis = createdAtEpochMillis,
                    cacheNotification = notification,
                ),
            ),
        )
        applicationContext.cacheSpecialCareNotifications(
            accountId = session.id,
            notifications = listOf(notification),
        )
        return true
    }

    private fun String.toEpochMillisOrNull(): Long? {
        return runCatching { Instant.parse(this).toEpochMilli() }.getOrNull()
    }

    private fun startRealtimeForegroundSafely(): Boolean {
        val notification = realtimeNotification()
        return runCatching {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                startForeground(
                    SERVICE_NOTIFICATION_ID,
                    notification,
                    ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC,
                )
            } else {
                startForeground(SERVICE_NOTIFICATION_ID, notification)
            }
        }.isSuccess
    }

    private fun realtimeNotification(): Notification {
        return NotificationCompat.Builder(applicationContext, REALTIME_SERVICE_CHANNEL_ID)
            .setSmallIcon(R.drawable.dc_icon)
            .setLargeIcon(BitmapFactory.decodeResource(resources, R.drawable.dc_icon))
            .setContentTitle("HHHL 实时同步中")
            .setContentText("正在实时接收消息和特别关心提醒")
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .setCategory(NotificationCompat.CATEGORY_SERVICE)
            .setOngoing(true)
            .setShowWhen(false)
            .build()
    }

    private fun renewRealtimeWakeLock() {
        val powerManager = getSystemService(Context.POWER_SERVICE) as? PowerManager ?: return
        val wakeLock = realtimeWakeLock ?: powerManager.newWakeLock(
            PowerManager.PARTIAL_WAKE_LOCK,
            "$packageName:RealtimeNotificationService",
        ).apply {
            setReferenceCounted(false)
            realtimeWakeLock = this
        }
        runCatching {
            wakeLock.acquire(REALTIME_WAKE_LOCK_TIMEOUT_MS)
        }
    }

    private fun releaseRealtimeWakeLock() {
        val wakeLock = realtimeWakeLock ?: return
        runCatching {
            if (wakeLock.isHeld) wakeLock.release()
        }
        realtimeWakeLock = null
    }

    private fun noteRealtimeActivity() {
        lastRealtimeActivityAt = System.currentTimeMillis()
    }

    private fun idleSyncIntervalMs(baseMs: Long, maxMs: Long): Long {
        val idleMs = System.currentTimeMillis() - lastRealtimeActivityAt
        if (idleMs < IDLE_BACKOFF_START_MS) return baseMs
        val steps = ((idleMs - IDLE_BACKOFF_START_MS) / IDLE_BACKOFF_STEP_MS)
            .toInt()
            .coerceAtMost(IDLE_BACKOFF_MAX_STEPS)
        var intervalMs = baseMs
        repeat(steps) { intervalMs *= 2L }
        return intervalMs.coerceAtMost(maxMs)
    }

    private fun MainStreamingEvent.isRealtimeActivitySignal(): Boolean = when (this) {
        is MainStreamingEvent.NotificationReceived,
        is MainStreamingEvent.ChatMessageReceived,
        is MainStreamingEvent.ChatMessageDeleted,
        MainStreamingEvent.NewChatMessage,
        MainStreamingEvent.UnreadNotification,
        MainStreamingEvent.ReadAllNotifications,
        is MainStreamingEvent.TimelineNote,
        -> true
        else -> false
    }

    private fun ChatStreamingEvent.isRealtimeActivitySignal(): Boolean = when (this) {
        is ChatStreamingEvent.MessageReceived,
        is ChatStreamingEvent.MessageDeleted,
        -> true
        else -> false
    }

    private fun ChatMessage.directPeerId(currentUserId: String?): String? {
        val cleanCurrentUserId = currentUserId?.trim().orEmpty()
        val recipientId = toUserId?.trim()?.takeIf { it.isNotEmpty() }
            ?: toUser?.id?.trim()?.takeIf { it.isNotEmpty() }
            ?: return null
        return when {
            cleanCurrentUserId.isNotBlank() && fromUser.id == cleanCurrentUserId -> recipientId
            cleanCurrentUserId.isNotBlank() && recipientId == cleanCurrentUserId -> fromUser.id
            cleanCurrentUserId.isBlank() && fromUser.id.isNotBlank() -> fromUser.id
            else -> null
        }?.takeIf { it.isNotBlank() }
    }

    companion object {
        @Volatile
        private var instanceRunning = false

        @Volatile
        internal var hibernatedDueToIdle = false
            private set

        internal fun isRunning(): Boolean = instanceRunning

        private const val SERVICE_NOTIFICATION_ID = 1001
        private const val RECONNECT_DELAY_MS = 3_000L
        private const val TIMELINE_SYNC_DEBOUNCE_MS = 2_000L
        private const val CHAT_SYNC_DEBOUNCE_MS = 3_000L
        private const val CHAT_EVENT_RECHECK_DELAY_MS = 1_500L
        private const val CHAT_TARGET_REFRESH_INTERVAL_MS = 30_000L
        private const val CHAT_TARGET_REFRESH_MAX_INTERVAL_MS = 300_000L
        private const val NOTIFICATION_EVENT_RECHECK_DELAY_MS = 1_500L
        private const val POLLING_SAFETY_INTERVAL_MS = 30_000L
        private const val POLLING_SAFETY_MAX_INTERVAL_MS = 300_000L
        private const val WAKE_LOCK_RENEW_INTERVAL_MS = 60_000L
        private const val IDLE_HIBERNATE_AFTER_MS = 30 * 60 * 1_000L
        private const val IDLE_BACKOFF_START_MS = 10 * 60 * 1_000L
        private const val IDLE_BACKOFF_STEP_MS = 5 * 60 * 1_000L
        private const val IDLE_BACKOFF_MAX_STEPS = 3
        private const val REALTIME_WAKE_LOCK_TIMEOUT_MS = 2 * 60 * 1000L
        private const val MAX_REALTIME_SEEN_IDS = 1_000
        private const val MAX_STREAMING_CHANNELS = 4

        fun start(context: Context): Boolean {
            return tryStart(context)
        }

        fun tryStart(context: Context): Boolean {
            val appContext = context.applicationContext
            val intent = Intent(context.applicationContext, RealtimeNotificationService::class.java)
            return runCatching {
                ensureChannels(appContext)
                ContextCompat.startForegroundService(appContext, intent)
                true
            }.getOrDefault(false)
        }

        fun stop(context: Context) {
            val intent = Intent(context.applicationContext, RealtimeNotificationService::class.java)
            context.applicationContext.stopService(intent)
        }
    }
}

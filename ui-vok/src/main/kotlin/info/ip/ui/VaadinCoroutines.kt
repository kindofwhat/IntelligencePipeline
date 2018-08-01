package info.ip.ui

import com.vaadin.flow.component.UI
import com.vaadin.flow.server.Command
import kotlinx.coroutines.experimental.CancellationException
import kotlinx.coroutines.experimental.CoroutineDispatcher
import kotlinx.coroutines.experimental.CoroutineExceptionHandler
import kotlinx.coroutines.experimental.Job
import kotlin.coroutines.experimental.CoroutineContext


fun checkUIThread() {
    require(UI.getCurrent() != null) { "Not running in Vaadin UI thread" }
}

/**
 * Implements [CoroutineDispatcher] on top of Vaadin [UI] and makes sure that all coroutine code runs in the UI thread.
 * Actions done in the UI thread are then automatically pushed by Vaadin Push to the browser.
 */
private data class VaadinDispatcher(val ui: UI?) : CoroutineDispatcher() {
    override fun dispatch(context: CoroutineContext, block: Runnable) {
        ui?.access(RunnableCommand(block))
    }
}

private class RunnableCommand(val runnable: Runnable):Command {
    override fun execute() {
        runnable.run()
    }
}

/**
 * If the coroutine fails, redirect the exception to the Vaadin Error Handler (the [UI.errorHandler] if specified; if not,
 * Vaadin will just log the exception).
 */
private data class VaadinExceptionHandler(val ui: UI?) : CoroutineExceptionHandler {
    override val key: CoroutineContext.Key<*>
        get() = CoroutineExceptionHandler

    override fun handleException(context: CoroutineContext, exception: Throwable) {
        // ignore CancellationException (they are normal means to terminate a coroutine)
        if (exception is CancellationException) return
        // try cancel job in the context
        context[Job]?.cancel(exception)
        // send the exception to Vaadin
        ui?.access { throw exception }
    }
}

/**
 * Provides the Vaadin Coroutine context for given [ui] (or the current one if none specified).
 */
fun vaadin(ui: UI = UI.getCurrent()) = VaadinDispatcher(ui) + VaadinExceptionHandler(ui)

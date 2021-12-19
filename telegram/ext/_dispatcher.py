#!/usr/bin/env python
#
# A library that provides a Python interface to the Telegram Bot API
# Copyright (C) 2015-2021
# Leandro Toledo de Souza <devs@python-telegram-bot.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser Public License for more details.
#
# You should have received a copy of the GNU Lesser Public License
# along with this program.  If not, see [http://www.gnu.org/licenses/].
"""This module contains the Dispatcher class."""
import asyncio
import inspect
import logging
from asyncio import Event
from collections import defaultdict
from pathlib import Path
from threading import Lock
from types import TracebackType
from typing import (
    Callable,
    DefaultDict,
    Dict,
    List,
    Optional,
    Union,
    Generic,
    TypeVar,
    TYPE_CHECKING,
    Coroutine,
    Sequence,
    Any,
    Type,
)

from telegram import Update
from telegram._utils.asyncio import run_non_blocking
from telegram.error import TelegramError
from telegram.ext import BasePersistence, ContextTypes, ExtBot
from telegram.ext._handler import Handler
from telegram.ext._callbackdatacache import CallbackDataCache
from telegram._utils.defaultvalue import DefaultValue, DEFAULT_FALSE
from telegram._utils.warnings import warn
from telegram.ext._utils.types import CCT, UD, CD, BD, BT, JQ, PT, HandlerCallback
from telegram.ext._utils.stack import was_called_by

if TYPE_CHECKING:
    from telegram.ext._jobqueue import Job
    from telegram.ext._builders import InitDispatcherBuilder

DEFAULT_GROUP: int = 0

_UT = TypeVar('_UT')
_DispType = TypeVar('_DispType', bound="Dispatcher")
_PooledRT = TypeVar('_PooledRT')
_STOP_SIGNAL = object()

_logger = logging.getLogger(__name__)


class DispatcherHandlerStop(Exception):
    """
    Raise this in a handler or an error handler to prevent execution of any other handler (even in
    different group).

    In order to use this exception in a :class:`telegram.ext.ConversationHandler`, pass the
    optional ``state`` parameter instead of returning the next state:

    .. code-block:: python

        def callback(update, context):
            ...
            raise DispatcherHandlerStop(next_state)

    Note:
        Has no effect, if the handler or error handler is run asynchronously.

    Attributes:
        state (:obj:`object`): Optional. The next state of the conversation.

    Args:
        state (:obj:`object`, optional): The next state of the conversation.
    """

    __slots__ = ('state',)

    def __init__(self, state: object = None) -> None:
        super().__init__()
        self.state = state


class Dispatcher(Generic[BT, CCT, UD, CD, BD, JQ, PT]):
    """This class dispatches all kinds of updates to its registered handlers.

    Note:
         This class may not be initialized directly. Use :class:`telegram.ext.DispatcherBuilder` or
         :meth:`builder` (for convenience).

    .. versionchanged:: 14.0
        Initialization is now done through the :class:`telegram.ext.DispatcherBuilder`.

    Attributes:
        bot (:class:`telegram.Bot`): The bot object that should be passed to the handlers.
        update_queue (:class:`asyncio.Queue`): The synchronized queue that will contain the
            updates.
        job_queue (:class:`telegram.ext.JobQueue`): Optional. The :class:`telegram.ext.JobQueue`
            instance to pass onto handler callbacks.
        workers (:obj:`int`, optional): Number of maximum concurrent worker threads for the
            ``@run_async`` decorator and :meth:`run_async`.
        user_data (:obj:`defaultdict`): A dictionary handlers can use to store data for the user.
        chat_data (:obj:`defaultdict`): A dictionary handlers can use to store data for the chat.
        bot_data (:obj:`dict`): A dictionary handlers can use to store data for the bot.
        persistence (:class:`telegram.ext.BasePersistence`): Optional. The persistence class to
            store data that should be persistent over restarts.
        handlers (Dict[:obj:`int`, List[:class:`telegram.ext.Handler`]]): A dictionary mapping each
            handler group to the list of handlers registered to that group.

            .. seealso::
                :meth:`add_handler`
        groups (List[:obj:`int`]): A list of all handler groups that have handlers registered.

            .. seealso::
                :meth:`add_handler`
        error_handlers (Dict[:obj:`callable`, :obj:`bool`]): A dict, where the keys are error
            handlers and the values indicate whether they are to be run asynchronously via
            :meth:`run_async`.

            .. seealso::
                :meth:`add_error_handler`

    """

    # Allowing '__weakref__' creation here since we need it for the JobQueue
    __slots__ = (
        '__weakref__',
        'workers',
        'persistence',
        'update_queue',
        'job_queue',
        'user_data',
        'chat_data',
        'bot_data',
        '_update_persistence_lock',
        'handlers',
        'groups',
        'error_handlers',
        '_running',
        '__run_asyncio_task_counter',
        '__run_asyncio_task_condition',
        '__update_fetcher_task',
        'bot',
        'context_types',
        'process_asyncio',
    )

    def __init__(
        self: 'Dispatcher[BT, CCT, UD, CD, BD, JQ, PT]',
        *,
        bot: BT,
        update_queue: asyncio.Queue,
        job_queue: JQ,
        workers: int,
        persistence: PT,
        context_types: ContextTypes[CCT, UD, CD, BD],
        stack_level: int = 4,
    ):
        if not was_called_by(
            inspect.currentframe(), Path(__file__).parent.resolve() / '_builders.py'
        ):
            warn(
                '`Dispatcher` instances should be built via the `DispatcherBuilder`.',
                stacklevel=2,
            )

        self.bot = bot
        self.update_queue = update_queue
        self.job_queue = job_queue
        self.workers = workers
        self.context_types = context_types
        self.process_asyncio = True

        if self.job_queue:
            self.job_queue.set_dispatcher(self)

        if self.workers < 1:
            warn(
                'Asynchronous callbacks can not be processed without at least one worker thread.',
                stacklevel=stack_level,
            )

        self.user_data: DefaultDict[int, UD] = defaultdict(self.context_types.user_data)
        self.chat_data: DefaultDict[int, CD] = defaultdict(self.context_types.chat_data)
        self.bot_data = self.context_types.bot_data()
        self.persistence: Optional[BasePersistence] = None
        self._update_persistence_lock = Lock()
        self._initialize_persistence(persistence)

        self.handlers: Dict[int, List[Handler]] = {}
        self.groups: List[int] = []
        self.error_handlers: Dict[Callable, Union[bool, DefaultValue]] = {}

        # A number of low-level helpers for the internal logic
        self._running = False
        self.__update_fetcher_task: Optional[asyncio.Task] = None
        self.__run_asyncio_task_counter = 0
        self.__run_asyncio_task_condition = asyncio.Condition()

    @property
    def running(self) -> bool:
        """:obj:`bool`: Indicates if this dispatcher is running.

        .. seealso::
            :meth:`start`, :meth:`stop`
        """
        return self._running

    async def initialize(self) -> None:
        await self.bot.initialize()

    async def shutdown(self) -> None:
        await self.bot.shutdown()

    async def __aenter__(self: _DispType) -> _DispType:
        try:
            await self.initialize()
            return self
        except Exception as exc:
            await self.shutdown()
            raise exc

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        # Make sure not to return `True` so that exceptions are not suppressed
        # https://docs.python.org/3/reference/datamodel.html?#object.__aexit__
        await self.shutdown()

    def _initialize_persistence(self, persistence: Optional[BasePersistence]) -> None:
        if not persistence:
            return

        if not isinstance(persistence, BasePersistence):
            raise TypeError("persistence must be based on telegram.ext.BasePersistence")

        self.persistence = persistence
        # This raises an exception if persistence.store_data.callback_data is True
        # but self.bot is not an instance of ExtBot - so no need to check that later on
        self.persistence.set_bot(self.bot)

        if self.persistence.store_data.user_data:
            self.user_data = self.persistence.get_user_data()
            if not isinstance(self.user_data, defaultdict):
                raise ValueError("user_data must be of type defaultdict")
        if self.persistence.store_data.chat_data:
            self.chat_data = self.persistence.get_chat_data()
            if not isinstance(self.chat_data, defaultdict):
                raise ValueError("chat_data must be of type defaultdict")
        if self.persistence.store_data.bot_data:
            self.bot_data = self.persistence.get_bot_data()
            if not isinstance(self.bot_data, self.context_types.bot_data):
                raise ValueError(
                    f"bot_data must be of type {self.context_types.bot_data.__name__}"
                )
        if self.persistence.store_data.callback_data:
            persistent_data = self.persistence.get_callback_data()
            if persistent_data is not None:
                if not isinstance(persistent_data, tuple) and len(persistent_data) != 2:
                    raise ValueError('callback_data must be a 2-tuple')
                # Mypy doesn't know that persistence.set_bot (see above) already checks that
                # self.bot is an instance of ExtBot if callback_data should be stored ...
                self.bot.callback_data_cache = CallbackDataCache(  # type: ignore[attr-defined]
                    self.bot,  # type: ignore[arg-type]
                    self.bot.callback_data_cache.maxsize,  # type: ignore[attr-defined]
                    persistent_data=persistent_data,
                )

    async def __increment_run_asyncio_task_counter(self) -> None:
        async with self.__run_asyncio_task_condition:
            self.__run_asyncio_task_counter += 1

    async def __decrement_run_asyncio_task_counter(self) -> None:
        async with self.__run_asyncio_task_condition:
            self.__run_asyncio_task_counter -= 1
            if self.__run_asyncio_task_counter <= 0:
                self.__run_asyncio_task_condition.notify_all()

    @staticmethod
    def builder() -> 'InitDispatcherBuilder':
        """Convenience method. Returns a new :class:`telegram.ext.DispatcherBuilder`.

        .. versionadded:: 14.0
        """
        # Unfortunately this needs to be here due to cyclical imports
        from telegram.ext import DispatcherBuilder  # pylint: disable=import-outside-toplevel

        return DispatcherBuilder()

    async def __pooled_wrapper(
        self,
        func: Callable[..., Union[_PooledRT, Coroutine[Any, Any, _PooledRT]]],
        args: Sequence[object],
        kwargs: Dict[str, object],
        update: Optional[object],
    ) -> Optional[_PooledRT]:
        try:
            return await self._pooled(func=func, args=args, kwargs=kwargs, update=update)
        finally:
            await self.__decrement_run_asyncio_task_counter()

    async def _pooled(
        self,
        func: Callable[..., Union[_PooledRT, Coroutine[Any, Any, _PooledRT]]],
        args: Sequence[object],
        kwargs: Dict[str, object],
        update: Optional[object],
    ) -> Optional[_PooledRT]:
        try:
            result = await run_non_blocking(func=func, args=args, kwargs=kwargs)

            self.update_persistence(update=update)
            return result

        except Exception as exception:
            if isinstance(exception, DispatcherHandlerStop):
                warn(
                    'DispatcherHandlerStop is not supported with async functions; '
                    f'func: {func.__qualname__}',
                )
                return None

            # Avoid infinite recursion of error handlers.
            if func in self.error_handlers:
                _logger.exception(
                    'An error was raised and an uncaught error was raised while '
                    'handling the error with an error_handler.',
                    exc_info=exception,
                )
                return None

            # If we arrive here, an exception happened in the task and was neither
            # DispatcherHandlerStop nor raised by an error handler. So we can and must handle it
            await self.dispatch_error(update, exception, asyncio_args=args, asyncio_kwargs=kwargs)
            return None

    async def run_async(
        self,
        func: Callable[..., Union[_PooledRT, Coroutine[Any, Any, _PooledRT]]],
        *args: object,
        update: object = None,
        **kwargs: object,
    ) -> 'asyncio.Task[Optional[_PooledRT]]':
        """
        Queue a function (with given args/kwargs) to be run asynchronously. Exceptions raised
        by the function will be handled by the error handlers registered with
        :meth:`add_error_handler`.

        Warning:
            * If you're using ``@run_async``/:meth:`run_async` you cannot rely on adding custom
              attributes to :class:`telegram.ext.CallbackContext`. See its docs for more info.
            * Calling a function through :meth:`run_async` from within an error handler can lead to
              an infinite error handling loop.

        Args:
            func (:obj:`callable`): The function to run in the thread.
            *args (:obj:`tuple`, optional): Arguments to ``func``.
            update (:class:`telegram.Update` | :obj:`object`, optional): The update associated with
                the functions call. If passed, it will be available in the error handlers, in case
                an exception is raised by :attr:`func`.
            **kwargs (:obj:`dict`, optional): Keyword arguments to ``func``.

        Returns:
            Promise

        """
        task = asyncio.create_task(
            self.__pooled_wrapper(func=func, args=args, kwargs=kwargs, update=update)
        )

        # Keep a track of how many tasks are running so that we can wait for them to finish
        # on shutdown
        await self.__increment_run_asyncio_task_counter()

        return task

    def start(self, ready: Event = None) -> None:
        """Thread target of thread 'dispatcher'.

        Runs in background and processes the update queue. Also starts :attr:`job_queue`, if set.

        Args:
            ready (:obj:`asyncio.Event`, optional): If specified, the event will be set once the
                dispatcher is ready.

        """
        if self.running:
            _logger.warning('already running')
            if ready is not None:
                ready.set()
            return

        self.__update_fetcher_task = asyncio.create_task(
            self._update_fetcher(), name=f'Dispatcher:{self.bot.id}:update_fetcher'
        )
        self._running = True
        _logger.debug('Dispatcher started')

        if self.job_queue:
            self.job_queue.start()
            _logger.debug('JobQueue started')

        if ready is not None:
            ready.set()

    async def stop(self) -> None:
        """Stops the process after processing any pending updates or tasks created by
        :meth:`run_asyncio`. Also stops :attr:`job_queue`, if set.
        Finally, calls :meth:`update_persistence` and :meth:`BasePersistence.flush` on
        :attr:`persistence`, if set.

        Warning:
            Once this method is called, no more updates will be fetched from :attr:`update_queue`,
            even if it's not empty.
        """
        if self.running:

            # Stop listening for new updates and handle all pending ones
            await self.update_queue.put(_STOP_SIGNAL)
            if self.__update_fetcher_task:
                await self.__update_fetcher_task
            _logger.debug("Dispatcher stopped fetching of updates.")

            # Wait for pending `run_async` tasks
            async with self.__run_asyncio_task_condition:
                if self.__run_asyncio_task_counter > 0:
                    _logger.debug('Waiting for `run_async` calls to be processed')
                    await self.__run_asyncio_task_condition.wait()

            self._running = False

            self.update_persistence()
            if self.persistence:
                self.persistence.flush()
                _logger.debug('Updated and flushed persistence')

            if self.job_queue:
                _logger.debug('Waiting for running jobs to finish')
                self.job_queue.stop(wait=True)
                _logger.debug('JobQueue stopped')

    async def _update_fetcher(self) -> None:
        # Continuously fetch updates from the queue. Exit only once the signal object is found.
        while True:
            try:
                update = await self.update_queue.get()

                if update is _STOP_SIGNAL:
                    return

                _logger.debug('Processing update %s', update)

                if self.process_asyncio:
                    asyncio.create_task(self.__process_update_wrapper(update))
                else:
                    await self.__process_update_wrapper(update)
            except Exception as exc:
                _logger.exception('updater fetcher got exception', exc_info=exc)

    async def __process_update_wrapper(self, update: object) -> None:
        await self.process_update(update)
        self.update_queue.task_done()

    async def process_update(self, update: object) -> None:
        """Processes a single update and updates the persistence.

        Note:
            If the update is handled by least one synchronously running handlers (i.e.
            ``run_async=False``), :meth:`update_persistence` is called *once* after all handlers
            synchronous handlers are done. Each asynchronously running handler will trigger
            :meth:`update_persistence` on its own.

        Args:
            update (:class:`telegram.Update` | :obj:`object` | \
                :class:`telegram.error.TelegramError`):
                The update to process.

        """
        # An error happened while polling
        if isinstance(update, TelegramError):
            await self.dispatch_error(None, update)
            return

        context = None
        handled = False
        sync_modes = []

        for group in self.groups:
            try:
                for handler in self.handlers[group]:
                    check = handler.check_update(update)
                    if check is not None and check is not False:
                        if not context:
                            context = self.context_types.context.from_update(update, self)
                            context.refresh_data()
                        handled = True
                        sync_modes.append(handler.run_async)
                        await handler.handle_update(update, self, check, context)
                        break

            # Stop processing with any other handler.
            except DispatcherHandlerStop:
                _logger.debug('Stopping further handlers due to DispatcherHandlerStop')
                self.update_persistence(update=update)
                break

            # Dispatch any error.
            except Exception as exc:
                if await self.dispatch_error(update, exc):
                    _logger.debug('Error handler stopped further handlers.')
                    break

        # Update persistence, if handled
        handled_only_async = all(sync_modes)
        if handled:
            # Respect default settings
            if (
                all(mode is DEFAULT_FALSE for mode in sync_modes)
                and isinstance(self.bot, ExtBot)
                and self.bot.defaults
            ):
                handled_only_async = self.bot.defaults.run_async
            # If update was only handled by async handlers, we don't need to update here
            if not handled_only_async:
                self.update_persistence(update=update)

    def add_handler(self, handler: Handler[_UT, CCT], group: int = DEFAULT_GROUP) -> None:
        """Register a handler.

        TL;DR: Order and priority counts. 0 or 1 handlers per group will be used. End handling of
        update with :class:`telegram.ext.DispatcherHandlerStop`.

        A handler must be an instance of a subclass of :class:`telegram.ext.Handler`. All handlers
        are organized in groups with a numeric value. The default group is 0. All groups will be
        evaluated for handling an update, but only 0 or 1 handler per group will be used. If
        :class:`telegram.ext.DispatcherHandlerStop` is raised from one of the handlers, no further
        handlers (regardless of the group) will be called.

        The priority/order of handlers is determined as follows:

          * Priority of the group (lower group number == higher priority)
          * The first handler in a group which should handle an update (see
            :attr:`telegram.ext.Handler.check_update`) will be used. Other handlers from the
            group will not be used. The order in which handlers were added to the group defines the
            priority.

        Args:
            handler (:class:`telegram.ext.Handler`): A Handler instance.
            group (:obj:`int`, optional): The group identifier. Default is 0.

        """
        # Unfortunately due to circular imports this has to be here
        # pylint: disable=import-outside-toplevel
        from telegram.ext._conversationhandler import ConversationHandler

        if not isinstance(handler, Handler):
            raise TypeError(f'handler is not an instance of {Handler.__name__}')
        if not isinstance(group, int):
            raise TypeError('group is not int')
        # For some reason MyPy infers the type of handler is <nothing> here,
        # so for now we just ignore all the errors
        if (
            isinstance(handler, ConversationHandler)
            and handler.persistent  # type: ignore[attr-defined]
            and handler.name  # type: ignore[attr-defined]
        ):
            if not self.persistence:
                raise ValueError(
                    f"ConversationHandler {handler.name} "  # type: ignore[attr-defined]
                    f"can not be persistent if dispatcher has no persistence"
                )
            handler.persistence = self.persistence  # type: ignore[attr-defined]
            handler.conversations = (  # type: ignore[attr-defined]
                self.persistence.get_conversations(handler.name)  # type: ignore[attr-defined]
            )

        if group not in self.handlers:
            self.handlers[group] = []
            self.groups.append(group)
            self.groups = sorted(self.groups)

        self.handlers[group].append(handler)

    def remove_handler(self, handler: Handler, group: int = DEFAULT_GROUP) -> None:
        """Remove a handler from the specified group.

        Args:
            handler (:class:`telegram.ext.Handler`): A Handler instance.
            group (:obj:`object`, optional): The group identifier. Default is 0.

        """
        if handler in self.handlers[group]:
            self.handlers[group].remove(handler)
            if not self.handlers[group]:
                del self.handlers[group]
                self.groups.remove(group)

    def update_persistence(self, update: object = None) -> None:
        """Update :attr:`user_data`, :attr:`chat_data` and :attr:`bot_data` in :attr:`persistence`.

        Args:
            update (:class:`telegram.Update`, optional): The update to process. If passed, only the
                corresponding ``user_data`` and ``chat_data`` will be updated.
        """
        with self._update_persistence_lock:
            self.__update_persistence(update)

    def __update_persistence(self, update: object = None) -> None:
        if self.persistence:
            # We use list() here in order to decouple chat_ids from self.chat_data, as dict view
            # objects will change, when the dict does and we want to loop over chat_ids
            chat_ids = list(self.chat_data.keys())
            user_ids = list(self.user_data.keys())

            if isinstance(update, Update):
                if update.effective_chat:
                    chat_ids = [update.effective_chat.id]
                else:
                    chat_ids = []
                if update.effective_user:
                    user_ids = [update.effective_user.id]
                else:
                    user_ids = []

            if self.persistence.store_data.callback_data:
                try:
                    # Mypy doesn't know that persistence.set_bot (see above) already checks that
                    # self.bot is an instance of ExtBot if callback_data should be stored ...
                    self.persistence.update_callback_data(
                        self.bot.callback_data_cache.persistence_data  # type: ignore[attr-defined]
                    )
                except Exception as exc:
                    self.dispatch_error(update, exc)
            if self.persistence.store_data.bot_data:
                try:
                    self.persistence.update_bot_data(self.bot_data)
                except Exception as exc:
                    self.dispatch_error(update, exc)
            if self.persistence.store_data.chat_data:
                for chat_id in chat_ids:
                    try:
                        self.persistence.update_chat_data(chat_id, self.chat_data[chat_id])
                    except Exception as exc:
                        self.dispatch_error(update, exc)
            if self.persistence.store_data.user_data:
                for user_id in user_ids:
                    try:
                        self.persistence.update_user_data(user_id, self.user_data[user_id])
                    except Exception as exc:
                        self.dispatch_error(update, exc)

    def add_error_handler(
        self,
        callback: HandlerCallback[object, CCT, None],
        run_async: Union[bool, DefaultValue] = DEFAULT_FALSE,
    ) -> None:
        """Registers an error handler in the Dispatcher. This handler will receive every error
        which happens in your bot. See the docs of :meth:`dispatch_error` for more details on how
        errors are handled.

        Note:
            Attempts to add the same callback multiple times will be ignored.

        Args:
            callback (:obj:`callable`): The callback function for this error handler. Will be
                called when an error is raised. Callback signature:
                ``def callback(update: object, context: CallbackContext)``.
                The error that happened will be present in ``context.error``.
            run_async (:obj:`bool`, optional): Whether this handlers callback should be run
                asynchronously using :meth:`run_async`. Defaults to :obj:`False`.
        """
        if callback in self.error_handlers:
            _logger.warning('The callback is already registered as an error handler. Ignoring.')
            return

        if (
            run_async is DEFAULT_FALSE
            and isinstance(self.bot, ExtBot)
            and self.bot.defaults
            and self.bot.defaults.run_async
        ):
            run_async = True

        self.error_handlers[callback] = run_async

    def remove_error_handler(self, callback: Callable[[object, CCT], None]) -> None:
        """Removes an error handler.

        Args:
            callback (:obj:`callable`): The error handler to remove.

        """
        self.error_handlers.pop(callback, None)

    async def dispatch_error(
        self,
        update: Optional[object],
        error: Exception,
        job: 'Job' = None,
        asyncio_args: Sequence[object] = None,
        asyncio_kwargs: Dict[str, object] = None,
    ) -> bool:
        """Dispatches an error by passing it to all error handlers registered with
        :meth:`add_error_handler`. If one of the error handlers raises
        :class:`telegram.ext.DispatcherHandlerStop`, the update will not be handled by other error
        handlers or handlers (even in other groups). All other exceptions raised by an error
        handler will just be logged.

        .. versionchanged:: 14.0

            * Exceptions raised by error handlers are now properly logged.
            * :class:`telegram.ext.DispatcherHandlerStop` is no longer reraised but converted into
              the return value.

        Args:
            update (:obj:`object` | :class:`telegram.Update`): The update that caused the error.
            error (:obj:`Exception`): The error that was raised.
            job (:class:`telegram.ext.Job`, optional): The job that caused the error.

                .. versionadded:: 14.0

        Returns:
            :obj:`bool`: :obj:`True` if one of the error handlers raised
                :class:`telegram.ext.DispatcherHandlerStop`. :obj:`False`, otherwise.
        """

        if self.error_handlers:
            for (
                callback,
                run_async,
            ) in self.error_handlers.items():  # pylint: disable=redefined-outer-name
                context = self.context_types.context.from_error(
                    update=update,
                    error=error,
                    dispatcher=self,
                    async_args=asyncio_args,
                    async_kwargs=asyncio_kwargs,
                    job=job,
                )
                if run_async:
                    await self.run_async(callback, update, context, update=update)
                else:
                    try:
                        await callback(update, context)
                    except DispatcherHandlerStop:
                        return True
                    except Exception as exc:
                        _logger.exception(
                            'An error was raised and an uncaught error was raised while '
                            'handling the error with an error_handler.',
                            exc_info=exc,
                        )
            return False

        _logger.exception('No error handlers are registered, logging exception.', exc_info=error)
        return False

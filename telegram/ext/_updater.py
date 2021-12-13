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
"""This module contains the class Updater, which tries to make creating Telegram bots intuitive."""
import asyncio
import inspect
import logging
import signal
from fileinput import FileInput
from pathlib import Path
from threading import Lock
from typing import (
    Any,
    Callable,
    List,
    Optional,
    Union,
    Generic,
    TypeVar,
    TYPE_CHECKING,
    Coroutine,
    Tuple,
)

from telegram.error import InvalidToken, RetryAfter, TimedOut, Forbidden, TelegramError
from telegram._utils.warnings import warn
from telegram.ext import Dispatcher
from telegram.ext._utils.stack import was_called_by
from telegram.ext._utils.types import BT

if TYPE_CHECKING:
    from telegram.ext._builders import InitUpdaterBuilder


DT = TypeVar('DT', bound=Union[None, Dispatcher])


class Updater(Generic[BT, DT]):
    """
    This class, which employs the :class:`telegram.ext.Dispatcher`, provides a frontend to
    :class:`telegram.Bot` to the programmer, so they can focus on coding the bot. Its purpose is to
    receive the updates from Telegram and to deliver them to said dispatcher. It also runs in a
    separate thread, so the user can interact with the bot, for example on the command line. The
    dispatcher supports handlers for different kinds of data: Updates from Telegram, basic text
    commands and even arbitrary types. The updater can be started as a polling service or, for
    production, use a webhook to receive updates. This is achieved using the WebhookServer and
    WebhookHandler classes.

    Note:
         This class may not be initialized directly. Use :class:`telegram.ext.UpdaterBuilder` or
         :meth:`builder` (for convenience).

    .. versionchanged:: 14.0

        * Initialization is now done through the :class:`telegram.ext.UpdaterBuilder`.
        * Renamed ``user_sig_handler`` to :attr:`user_signal_handler`.
        * Removed the attributes ``job_queue``, and ``persistence`` - use the corresponding
          attributes of :attr:`dispatcher` instead.

    Attributes:
        bot (:class:`telegram.Bot`): The bot used with this Updater.
        user_signal_handler (:obj:`function`): Optional. Function to be called when a signal is
            received.

            .. versionchanged:: 14.0
                Renamed ``user_sig_handler`` to ``user_signal_handler``.
        update_queue (:class:`asyncio.Queue`): Queue for the updates.
        dispatcher (:class:`telegram.ext.Dispatcher`): Optional. Dispatcher that handles the
            updates and dispatches them to the handlers.
        running (:obj:`bool`): Indicates if the updater is running.

    """

    __slots__ = (
        'dispatcher',
        'user_signal_handler',
        'bot',
        'logger',
        'update_queue',
        'last_update_id',
        'running',
        'is_idle',
        'httpd',
        '__lock',
        '__asyncio_tasks',
    )

    def __init__(
        self: 'Updater[BT, DT]',
        *,
        user_signal_handler: Callable[[int, object], Any] = None,
        dispatcher: DT = None,
        bot: BT = None,
        update_queue: asyncio.Queue = None,
    ):
        if not was_called_by(
            inspect.currentframe(), Path(__file__).parent.resolve() / '_builders.py'
        ):
            warn(
                '`Updater` instances should be built via the `UpdaterBuilder`.',
                stacklevel=2,
            )

        self.user_signal_handler = user_signal_handler
        self.dispatcher = dispatcher
        if self.dispatcher:
            self.bot = self.dispatcher.bot
            self.update_queue = self.dispatcher.update_queue
        else:
            self.bot = bot
            self.update_queue = update_queue

        self.last_update_id = 0
        self.running = False
        self.is_idle = False
        self.httpd = None
        self.__lock = Lock()
        self.__asyncio_tasks: List[asyncio.Task] = []
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def builder() -> 'InitUpdaterBuilder':
        """Convenience method. Returns a new :class:`telegram.ext.UpdaterBuilder`.

        .. versionadded:: 14.0
        """
        # Unfortunately this needs to be here due to cyclical imports
        from telegram.ext import UpdaterBuilder  # pylint: disable=import-outside-toplevel

        return UpdaterBuilder()

    async def initialize(self) -> None:
        if self.dispatcher:
            await self.dispatcher.start()
        else:
            await self.bot.initialize()

    async def shutdown(self) -> None:
        if self.dispatcher:
            self.logger.debug('Requesting Dispatcher to stop...')
            await self.dispatcher.stop()
        else:
            await self.bot.shutdown()

    def _init_task(
        self, target: Callable[..., Coroutine], name: str, *args: object, **kwargs: object
    ) -> None:
        task = asyncio.create_task(
            coro=self._thread_wrapper(target, *args, **kwargs),
            # name=f"Bot:{self.bot.id}:{name}",
        )
        self.__asyncio_tasks.append(task)

    async def _thread_wrapper(self, target: Callable, *args: object, **kwargs: object) -> None:
        task_name = '# FIXME'
        self.logger.debug('%s - started', task_name)
        try:
            await target(*args, **kwargs)
        except Exception:
            self.logger.exception('unhandled exception in %s', task_name)
            raise
        self.logger.debug('%s - ended', task_name)

    # TODO: Probably drop `pool_connect` timeout again, because we probably want to just make
    #       sure that `getUpdates` always gets a connection without waiting
    async def start_polling(
        self,
        poll_interval: float = 0.0,
        timeout: int = 10,
        bootstrap_retries: int = -1,
        read_timeout: float = 2,
        write_timeout: float = None,
        connect_timeout: float = None,
        pool_timeout: float = None,
        allowed_updates: List[str] = None,
        drop_pending_updates: bool = None,
    ) -> Optional[asyncio.Queue]:
        """Starts polling updates from Telegram.

        .. versionchanged:: 14.0
            Removed the ``clean`` argument in favor of ``drop_pending_updates``.

        Args:
            poll_interval (:obj:`float`, optional): Time to wait between polling updates from
                Telegram in seconds. Default is ``0.0``.
            timeout (:obj:`float`, optional): Passed to :meth:`telegram.Bot.get_updates`.
            drop_pending_updates (:obj:`bool`, optional): Whether to clean any pending updates on
                Telegram servers before actually starting to poll. Default is :obj:`False`.

                .. versionadded :: 13.4
            bootstrap_retries (:obj:`int`, optional): Whether the bootstrapping phase of the
                :class:`telegram.ext.Updater` will retry on failures on the Telegram server.

                * < 0 - retry indefinitely (default)
                *   0 - no retries
                * > 0 - retry up to X times

            allowed_updates (List[:obj:`str`], optional): Passed to
                :meth:`telegram.Bot.get_updates`.
            read_timeout (:obj:`float` | :obj:`int`, optional): Grace time in seconds for receiving
                the reply from server. Will be added to the ``timeout`` value and used as the read
                timeout from server (Default: ``2``).

        Returns:
            :class:`asyncio.Queue`: The update queue that can be filled from the main thread.

        """
        with self.__lock:
            if not self.running:
                self.running = True

                # Create & start threads
                dispatcher_ready = asyncio.Event()
                polling_ready = asyncio.Event()

                self._init_task(
                    self._start_polling,
                    "updater",
                    poll_interval,
                    timeout,
                    read_timeout,
                    write_timeout,
                    connect_timeout,
                    pool_timeout,
                    bootstrap_retries,
                    drop_pending_updates,
                    allowed_updates,
                    ready=polling_ready,
                )

                self.logger.debug('Waiting for polling to start')
                await polling_ready.wait()
                # if self.dispatcher:
                #     self.logger.debug('Waiting for Dispatcher to start')
                #     await dispatcher_ready.wait()

                # Return the update queue so the main thread can insert updates
                return self.update_queue
            return None

    async def _start_polling(
        self,
        poll_interval: float,
        timeout: int,
        read_timeout: Optional[float],
        write_timeout: Optional[float],
        connect_timeout: Optional[float],
        pool_timeout: Optional[float],
        bootstrap_retries: int,
        drop_pending_updates: bool,
        allowed_updates: Optional[List[str]],
        ready: asyncio.Event = None,
    ) -> None:
        # Thread target of thread 'updater.start_polling()'. Runs in background, pulls
        # updates from Telegram and inserts them in the update queue of the
        # Dispatcher.

        self.logger.debug('Updater thread started (polling)')

        await self._bootstrap(
            bootstrap_retries,
            drop_pending_updates=drop_pending_updates,
            webhook_url='',
            allowed_updates=None,
        )

        self.logger.debug('Bootstrap done')

        async def polling_action_cb() -> bool:
            updates = await self.bot.get_updates(
                self.last_update_id,
                timeout=timeout,
                read_timeout=read_timeout,
                connect_timeout=connect_timeout,
                write_timeout=write_timeout,
                pool_timeout=pool_timeout,
                allowed_updates=allowed_updates,
            )

            if updates:
                if not self.running:
                    self.logger.debug('Updates ignored and will be pulled again on restart')
                else:
                    for update in updates:
                        await self.update_queue.put(update)
                    self.last_update_id = updates[-1].update_id + 1

            return True

        # TODO: rethink this. suggestion:
        #   • If we have a dispatcher, just call `dispatcher.dispatch_error`
        #   • Otherwise, log it
        async def polling_onerr_cb(exc: Exception) -> None:
            # Put the error into the update queue and let the Dispatcher
            # broadcast it
            await self.update_queue.put(exc)

        if ready is not None:
            ready.set()

        await self._network_loop_retry(
            polling_action_cb, polling_onerr_cb, 'getting Updates', poll_interval
        )

    async def _network_loop_retry(
        self,
        action_cb: Callable[..., Coroutine],
        onerr_cb: Callable[..., Coroutine],
        description: str,
        interval: float,
    ) -> None:
        """Perform a loop calling `action_cb`, retrying after network errors.

        Stop condition for loop: `self.running` evaluates :obj:`False` or return value of
        `action_cb` evaluates :obj:`False`.

        Args:
            action_cb (:obj:`callable`): Network oriented callback function to call.
            onerr_cb (:obj:`callable`): Callback to call when TelegramError is caught. Receives the
                exception object as a parameter.
            description (:obj:`str`): Description text to use for logs and exception raised.
            interval (:obj:`float` | :obj:`int`): Interval to sleep between each call to
                `action_cb`.

        """
        self.logger.debug('Start network loop retry %s', description)
        cur_interval = interval
        while self.running:
            try:
                if not await action_cb():
                    break
            except RetryAfter as exc:
                self.logger.info('%s', exc)
                cur_interval = 0.5 + exc.retry_after
            except TimedOut as toe:
                self.logger.debug('Timed out %s: %s', description, toe)
                # If failure is due to timeout, we should retry asap.
                cur_interval = 0
            except InvalidToken as pex:
                self.logger.error('Invalid token; aborting')
                raise pex
            except TelegramError as telegram_exc:
                self.logger.error('Error while %s: %s', description, telegram_exc)
                await onerr_cb(telegram_exc)
                cur_interval = self._increase_poll_interval(cur_interval)
            else:
                cur_interval = interval

            if cur_interval:
                await asyncio.sleep(cur_interval)

    @staticmethod
    def _increase_poll_interval(current_interval: float) -> float:
        # increase waiting times on subsequent errors up to 30secs
        if current_interval == 0:
            current_interval = 1
        elif current_interval < 30:
            current_interval *= 1.5
        else:
            current_interval = min(30.0, current_interval)
        return current_interval

    async def _bootstrap(
        self,
        max_retries: int,
        drop_pending_updates: bool,
        webhook_url: Optional[str],
        allowed_updates: Optional[List[str]],
        cert: FileInput = None,
        bootstrap_interval: float = 5,
        ip_address: str = None,
        max_connections: int = 40,
    ) -> None:
        retries = [0]

        async def bootstrap_del_webhook() -> bool:
            self.logger.debug('Deleting webhook')
            if drop_pending_updates:
                self.logger.debug('Dropping pending updates from Telegram server')
            await self.bot.delete_webhook(drop_pending_updates=drop_pending_updates)
            return False

        async def bootstrap_set_webhook() -> bool:
            self.logger.debug('Setting webhook')
            if drop_pending_updates:
                self.logger.debug('Dropping pending updates from Telegram server')
            await self.bot.set_webhook(
                url=webhook_url,
                certificate=cert,
                allowed_updates=allowed_updates,
                ip_address=ip_address,
                drop_pending_updates=drop_pending_updates,
                max_connections=max_connections,
            )
            return False

        async def bootstrap_onerr_cb(exc: Exception) -> None:
            if not isinstance(exc, Forbidden) and (max_retries < 0 or retries[0] < max_retries):
                retries[0] += 1
                self.logger.warning(
                    'Failed bootstrap phase; try=%s max_retries=%s', retries[0], max_retries
                )
            else:
                self.logger.error('Failed bootstrap phase after %s retries (%s)', retries[0], exc)
                raise exc

        # Dropping pending updates from TG can be efficiently done with the drop_pending_updates
        # parameter of delete/start_webhook, even in the case of polling. Also we want to make
        # sure that no webhook is configured in case of polling, so we just always call
        # delete_webhook for polling
        if drop_pending_updates or not webhook_url:
            await self._network_loop_retry(
                bootstrap_del_webhook,
                bootstrap_onerr_cb,
                'bootstrap del webhook',
                bootstrap_interval,
            )
            retries[0] = 0

        # Restore/set webhook settings, if needed. Again, we don't know ahead if a webhook is set,
        # so we set it anyhow.
        if webhook_url:
            await self._network_loop_retry(
                bootstrap_set_webhook,
                bootstrap_onerr_cb,
                'bootstrap set webhook',
                bootstrap_interval,
            )

    async def stop_fetching_updates(self) -> None:
        """Stops the polling/webhook thread, the dispatcher and the job queue."""
        with self.__lock:
            if self.running or (self.dispatcher and self.dispatcher.running):
                self.logger.debug(
                    'Stopping Updater %s...', 'and Dispatcher ' if self.dispatcher else ''
                )

                self.running = False

                self._stop_httpd()
                await self._join_tasks()

    def _stop_httpd(self) -> None:
        if self.httpd:
            self.logger.debug(
                'Waiting for current webhook connection to be '
                'closed... Send a Telegram message to the bot to exit '
                'immediately.'
            )
            self.httpd.shutdown()
            self.httpd = None

    async def _join_tasks(self) -> None:
        await asyncio.gather(*self.__asyncio_tasks)
        self.__asyncio_tasks = []

    def _signal_handler(self, signum, frame) -> None:
        self.is_idle = False
        if self.running:
            self.logger.info(
                'Received signal %s (%s), stopping...',
                signum,
                # signal.Signals is undocumented in py3.5-3.8, but existed nonetheless
                # https://github.com/python/cpython/pull/28628
                signal.Signals(signum),  # pylint: disable=no-member
            )
            asyncio.create_task(self.stop_fetching_updates())
            asyncio.create_task(self.shutdown())
            if self.user_signal_handler:
                self.user_signal_handler(signum, frame)
        else:
            self.logger.warning('Exiting immediately!')
            # pylint: disable=import-outside-toplevel, protected-access
            import os

            os._exit(1)

    async def idle(
        self, stop_signals: Union[List, Tuple] = (signal.SIGINT, signal.SIGTERM, signal.SIGABRT)
    ) -> None:
        """Blocks until one of the signals are received and stops the updater.

        Args:
            stop_signals (:obj:`list` | :obj:`tuple`): List containing signals from the signal
                module that should be subscribed to. :meth:`Updater.stop()` will be called on
                receiving one of those signals. Defaults to (``SIGINT``, ``SIGTERM``, SIGABRT``).

        """
        for sig in stop_signals:
            signal.signal(sig, self._signal_handler)

        self.is_idle = True

        while self.is_idle:
            await asyncio.sleep(1)

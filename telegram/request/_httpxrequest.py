#
#  A library that provides a Python interface to the Telegram Bot API
#  Copyright (C) 2015-2021
#  Leandro Toledo de Souza <devs@python-telegram-bot.org>
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser Public License for more details.
#
#  You should have received a copy of the GNU Lesser Public License
#  along with this program.  If not, see [http://www.gnu.org/licenses/].

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
"""This module contains methods to make POST and GET requests using the httpx library."""
from typing import Tuple, Optional

import httpx

from telegram.error import TimedOut, NetworkError
from telegram.request import BaseRequest, RequestData


# Note to future devs:
# Proxies are currently only tested manually. The httpx development docs have a nice guide on that:
# https://www.python-httpx.org/contributing/#development-proxy-setup (also saved on archive.org)
# That also works with socks5. Just pass `--mode socks5` to mitmproxy and the `verify` argument to
# AsyncProxyTransport.from_url


class HTTPXRequest(BaseRequest):
    """Implementation of :class`BaseRequest` using the library
    `httpx <https://www.python-httpx.org>`_`.

    Args:
        connection_pool_size (:obj:`int`, optional): Number of connections to keep in the
            connection pool. Default to :obj:`1`.
        proxy_url (:obj:`str`, optional): The URL to the proxy server. For example
            ``'http://127.0.0.1:3128'``. Defaults to :obj:`None`.
        connect_timeout (:obj:`float`, optional): The maximum amount of time (in seconds) to wait
            for a connection attempt to a server to succeed. :obj:`None` will set an infinite
            timeout for connection attempts. Defaults to ``5.0``.
        read_timeout (:obj:`float`, optional): The maximum amount of time (in seconds) to wait for
            a response from Telegram's server. :obj:`None` will set an infinite timeout. This value
            is usually overridden by the various methods of :class:`telegram.Bot`. Defaults to
            ``5.0``.
        write_timeout (:obj:`float`, optional): The maximum amount of time (in seconds) to wait for
            a write operation to complete (in terms of a network socket; i.e. POSTing a request or
            uploading a file).:obj:`None` will set an infinite timeout. Defaults to ``5.0``.
        pool_timeout (:obj:`float`, optional): Timeout waiting for a connection object to become
            available and returned from the connection pool. :obj:`None` will set an infinite
            timeout. Defaults to ``1.0``.

    """

    __slots__ = ('_client', '_connection_pool_size')

    def __init__(
        self,
        connection_pool_size: int = 1,
        proxy_url: str = None,
        connect_timeout: Optional[float] = 5.0,
        read_timeout: Optional[float] = 5.0,
        write_timeout: Optional[float] = 5.0,
        pool_timeout: Optional[float] = 1.0,
    ):
        timeout = httpx.Timeout(
            connect=connect_timeout,
            read=read_timeout,
            write=write_timeout,
            pool=pool_timeout,
        )
        self._connection_pool_size = connection_pool_size
        limits = httpx.Limits(
            max_connections=connection_pool_size, max_keepalive_connections=connection_pool_size
        )

        # Handle socks5 proxies
        if proxy_url and proxy_url.startswith('socks'):
            try:
                from httpx_socks import (  # pylint: disable=import-outside-toplevel
                    AsyncProxyTransport,
                )

                transport = AsyncProxyTransport.from_url(
                    proxy_url,
                )
                proxy_url = None
            except ImportError as exc:
                raise RuntimeError(
                    'Requirements missing for Socks5 support. Install PTB via '
                    '`pip install python-telegram-bot[socks]`.'
                ) from exc
        else:
            transport = None

        # TODO p0: Test client with proxy!
        # TODO p2: Document that this can also be specified via env vars
        #          https://www.python-httpx.org/advanced/#environment-variables
        self._client = httpx.AsyncClient(
            timeout=timeout,
            proxies=proxy_url,
            limits=limits,
            transport=transport,
        )

    @property
    def connection_pool_size(self) -> int:
        """See :attr:`BaseRequest.connection_pool_size`."""
        return self._connection_pool_size

    async def initialize(self) -> None:
        """See :meth:`BaseRequest.initialize`."""

    async def shutdown(self) -> None:
        """See :meth:`BaseRequest.stop`."""
        await self._client.aclose()

    async def do_request(
        self,
        method: str,
        url: str,
        request_data: RequestData = None,
        connect_timeout: float = None,
        read_timeout: float = None,
        write_timeout: float = None,
        pool_timeout: float = None,
    ) -> Tuple[int, bytes]:
        """See :meth:`BaseRequest.do_request`."""
        timeout = httpx.Timeout(
            connect=self._client.timeout.connect,
            read=self._client.timeout.read,
            write=self._client.timeout.write,
            pool=self._client.timeout.pool,
        )
        if read_timeout is not None:
            timeout.read = read_timeout
        if write_timeout is not None:
            timeout.write = write_timeout
        if connect_timeout is not None:
            timeout.connect = connect_timeout
        if pool_timeout is not None:
            timeout.pool = pool_timeout

        # TODO p0: On Linux, use setsockopt to properly set socket level keepalive.
        #          (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 120)
        #          (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30)
        #          (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 8)
        # TODO p4: Support setsockopt on lesser platforms than Linux.

        files = request_data.multipart_data if request_data else None
        data = request_data.json_parameters if request_data else None
        try:
            res = await self._client.request(
                method=method,
                url=url,
                headers={'User-Agent': self.USER_AGENT},
                timeout=timeout,
                files=files,
                data=data,
            )
        except httpx.TimeoutException as err:
            raise TimedOut() from err
        except httpx.HTTPError as err:
            # HTTPError must come last as its the base httpx exception class
            # TODO p4: do something smart here; for now just raise NetworkError
            raise NetworkError(f'httpx HTTPError: {err}') from err

        return res.status_code, res.content

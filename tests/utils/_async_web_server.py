#  Copyright 2016-2024. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import asyncio
import logging
import json
import sys

from typing import Optional
import urllib.parse

from aiohttp import web


logging.basicConfig(level=logging.INFO,
                    stream=sys.stderr, 
                    format='%(asctime)s - %(levelname)s - (PID:%(process)d) - %(message)s')
logger = logging.getLogger(__name__)

class AsyncWebServer:
    def __init__(self, host: Optional[str]='0.0.0.0', port:Optional[int]=8080) -> None:
        self._app = web.Application()
        self._host = host
        self._port = port
        self._app.add_routes([web.get('/test_get', self.handle_get_request),
                             web.post('/test_post', self.handle_post_request)])

    async def handle_get_request(self, request):
        path = request.match_info['path']
        query_params = request.query_string
        response_data = {
            'path': path,
            'query': urllib.parse.parse_qs(query_params)
        }
        return web.json_response(response_data)
    
    async def handle_post_request(self, request):
        try:
            received_json = await request.json()
            logger.info(f"Received JSON: {received_json}")
            return web.json_response({
                'status': 'success',
                'data': received_json
            })
        except json.JSONDecodeError:
            received_text = await request.text()
            msg = "POST request received, but data is not valid JSON. Showing as plain text."
            logger.error(msg)
            logger.error(f'Received text: {received_text}')
        except Exception as e:
            logger.error(f'An error occurred: {e}', exc_info=True)
            return web.Response(status=400, text="Bad Request")

    async def start(self):
        runner = web.AppRunner(self._app)
        await runner.setup()
        site = web.TCPSite(runner, self._host, self._port)
        await site.start()
        logger.info(f'Server running on http://{self._host}:{self._port}')

    async def stop(self):
        await self._app.shutdown()
        await self._app.cleanup()

async def run_server(host: str, port: int) -> None:
    server = AsyncWebServer(host=host, port=port)
    logger.info(f'Attempting to start server on {host}:{port}...')
    await server.start()
    logger.info('Server started. Listening for requests...')
    try:
        while True:
            await asyncio.sleep(300)
    except asyncio.CancelledError:
        logger.info('asyncio task cancelled (e.g., from SIGTERM). Shutting down.')
    except Exception as e:
        logger.error(f'Unexpected error: {e}', exc_info=True)
    finally:
        logger.info('Stopping server...')
        await server.stop()
        logger.info('Server stopped.')


if __name__ == '__main__':
    from argparse import ArgumentParser
    ap = ArgumentParser(description='Run Async Web Server')
    ap.add_argument('--host',
                    type=str,
                    default='127.0.0.1',
                    help='Host address to bind to (e.g., 127.0.0.1 for localhost only)')
    ap.add_argument('--port',
                    type=int,
                    default=8000,
                    help='Port number to listen on')
    options = ap.parse_args()
    try:
        asyncio.run(run_server(host=options.host, port=options.port))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.critical(f'Critical error: {e}', exc_info=True)
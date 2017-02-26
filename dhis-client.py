#!/usr/bin/env python3
# -*- mode: python3; coding: utf-8 -*-

import collections
import sys
import asyncio
import signal
import ssl
from urllib import request as url_request

HOSTNAME_MAP = {
    '91.227.223.214': 'dhis.org',
}
DHIS_URL = 'http://91.227.223.214/'
USERNAME = '31487'
PASSWORD = 'S3VS0FALH84KSW5'
INTERVAL = 300.0

original_match_hostname = ssl.match_hostname

def overridden_match_hostname(cert, hostname):
    fixed_hostname = HOSTNAME_MAP.get(hostname)
    
    if fixed_hostname is not None:
        hostname = fixed_hostname
    
    original_match_hostname(cert, hostname)

ssl.match_hostname = overridden_match_hostname

DhisCtx = collections.namedtuple('DhisCtx', (
    'dhis_url',
    'username',
    'password',
    'interval',
    'error_handler',
))

def blocking_dhis_update(dhis_ctx):
    password_mgr = url_request.HTTPPasswordMgrWithPriorAuth()
    
    password_mgr.add_password(
        realm=None,
        uri=dhis_ctx.dhis_url,
        user=dhis_ctx.username,
        passwd=dhis_ctx.password,
        is_authenticated=True,
    )
    
    opener = url_request.build_opener(
        url_request.HTTPBasicAuthHandler(
            password_mgr=password_mgr,
        ),
    )
    
    with opener.open(
                url_request.Request(
                    dhis_ctx.dhis_url,
                ),
                timeout=20.0,
            ) as res:
        res.read(10000000)

async def dhis_loop(dhis_ctx, loop=None):
    assert loop is not None
    
    while True:
        dhis_update_fut = loop.run_in_executor(
            None,
            blocking_dhis_update,
            dhis_ctx,
        )
        sleep_fut = asyncio.ensure_future(
            asyncio.sleep(INTERVAL, loop=loop),
            loop=loop,
        )
        
        try:
            await asyncio.wait(
                (dhis_update_fut,),
                loop=loop,
            )
            
            if dhis_ctx.error_handler is not None:
                upd_error = dhis_update_fut.exception()
                
                if upd_error is not None:
                    dhis_ctx.error_handler(upd_error)
            
            await asyncio.wait(
                (sleep_fut,),
                loop=loop,
            )
        finally:
            dhis_update_fut.cancel()
            sleep_fut.cancel()

def main():
    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event(loop=loop)
    
    def shutdown_handler():
        if not shutdown_event.is_set():
            print('shutdown event')
            
            shutdown_event.set()
    
    loop.add_signal_handler(signal.SIGINT, shutdown_handler)
    loop.add_signal_handler(signal.SIGTERM, shutdown_handler)
    
    def error_handler(error):
        print(f'error: {repr(error)}', file=sys.stderr)
    
    dhis_ctx = DhisCtx(
        dhis_url=DHIS_URL,
        username=USERNAME,
        password=PASSWORD,
        interval=INTERVAL,
        error_handler=error_handler,
    )
    
    dhis_loop_fut = asyncio.ensure_future(
        dhis_loop(dhis_ctx, loop=loop),
        loop=loop,
    )
    
    async def shutdown_event_handler():
        await shutdown_event.wait()
        
        dhis_loop_fut.cancel()
    
    asyncio.ensure_future(
        shutdown_event_handler(),
        loop=loop,
    )
    
    print('dhis loop begin')
    
    try:
        loop.run_until_complete(dhis_loop_fut)
    except asyncio.CancelledError:
        pass
    
    print('dhis loop end')

if __name__ == '__main__':
    main()

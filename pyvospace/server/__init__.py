##########################################################################################
# Fuzzing is a technique for amplifying race condition errors to make them more visible
import asyncio

FUZZ = False
FUZZ01 = False
FUZZ01_reached = False


async def fuzz(elapse=1):
    if FUZZ:
        await asyncio.sleep(elapse)


def set_fuzz(fuzz):
    global FUZZ
    FUZZ = fuzz


async def fuzz01(elapse=1):
    if FUZZ01:
        global FUZZ01_reached
        try:
            FUZZ01_reached = True
            await asyncio.sleep(elapse)
        finally:
            FUZZ01_reached = False


def set_fuzz01(fuzz):
    global FUZZ01
    FUZZ01 = fuzz


async def wait_fuzz01(timer=0.01):
    while FUZZ01_reached is False:
        await asyncio.sleep(timer)

###########################################################################################

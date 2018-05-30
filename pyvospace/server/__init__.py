##########################################################################################
# Fuzzing is a technique for amplifying race condition errors to make them more visible
import asyncio

FUZZ = False

async def fuzz(elapse=1):
    if FUZZ:
        await asyncio.sleep(elapse)

def set_fuzz(fuzz):
    global FUZZ
    FUZZ = fuzz

###########################################################################################

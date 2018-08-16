from setuptools import setup, find_packages

setup(name='pyvospace',
      version='0.1.0',
      packages=find_packages(),
      install_requires=['aiohttp',
                        'aiohttp_jinja2',
                        'aiohttp_security',
                        'aiohttp-session',
                        'asyncpg',
                        'cryptography',
                        'aio_pika',
                        'passlib',
                        'uvloop',
                        'aiofiles',
                        'riprova',
                        'lxml',
                        'cchardet',
                        'aiodns',
                        'aiojobs',
                        'requests'],
      entry_points={'console_scripts': [
          'posix_space = pyvospace.server.spaces.posix.space.__main__:main',
          'posix_storage = pyvospace.server.spaces.posix.storage.__main__:main']
      })
"""
Publish a new version:
$ git tag X.Y.Z -m "Release X.Y.Z"
$ git push --tags
$ pip install --upgrade twine wheel
$ python setup.py sdist bdist_wheel --universal
$ twine upload dist/*
"""

import codecs
import setuptools

ARQUE_VERSION = '1.0.5'
ARQUE_DOWNLOAD_URL = (
        'https://github.com/code-impactor/arque/releases/tag/' + ARQUE_VERSION
)


def read_file(filename):
    """
    Read a utf8 encoded text file and return its contents.
    """
    with codecs.open(filename, 'r', 'utf8') as f:
        return f.read()


setuptools.setup(
    name='arque',
    packages=['arque'],
    version=ARQUE_VERSION,
    description="Asyncio Reliable Queue (based on redis)",
    long_description=read_file('README.md'),
    long_description_content_type="text/markdown",
    license='MIT',
    author="Andrei Roskach",
    author_email="code.impactor@gmail.com",
    url="https://github.com/code-impactor/arque",
    download_url=ARQUE_DOWNLOAD_URL,
    keywords=[
        'asyncio', 'redis', 'reliable', 'queue', 'asynchronous', 'python', 'reliable-queue', 'work-queue', 'delay',
        'delayed', 'jobs', 'delayed-queue'
    ],
    install_requires=['aioredis >= 1.2.0'],
    platforms=['Any'],
    classifiers=[
        'Intended Audience :: Developers',
        "Programming Language :: Python :: 3.7",
        'Natural Language :: English',
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Topic :: Database',
        'Topic :: Software Development :: Libraries',
    ],
    python_requires='>=3.7'
)

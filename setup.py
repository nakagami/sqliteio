from setuptools import setup


classifiers = [
    'Development Status :: 3 - Alpha',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Topic :: Database',
]


setup(
    name="sqliteio",
    version="0.1.0",
    url='https://github.com/nakagami/sqliteio/',
    classifiers=classifiers,
    keywords=['SQLite3'],
    author='Hajime Nakagami',
    author_email='nakagami@gmail.com',
    description='SQLite3 I/O library',
    long_description=open('README.rst').read(),
    license="MIT",
    packages=['sqliteio'],
)

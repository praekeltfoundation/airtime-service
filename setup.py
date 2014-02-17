from setuptools import setup

setup(
    name="airtime-service",
    version="0.1.0",
    url='https://github.com/praekelt/airtime-service',
    license='MIT',
    description="A RESTful service for issuing airtime.",
    long_description=open('README.rst', 'r').read(),
    author='Praekelt Foundation',
    author_email='dev@praekeltfoundation.org',
    packages=["airtime_service"],
    install_requires=[
        "Twisted", "klein", "sqlalchemy", "alchimia>=0.4", "aludel==0.3",
    ],
)

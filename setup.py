from setuptools import setup, find_packages
from pathlib import Path

name = 'metrics_collector'
__version__ = ''
exec((Path(name) / '__version__.py').read_text())

setup(
    name=name,
    version=__version__,
    packages=find_packages(),
    include_package_data=True,
    install_requires=open('requirements.txt').read().split('\n'),
    url='',
    license='MIT',
    author='Daniel Engvall',
    author_email='daniel@engvalls.eu',
    description='Application for structuring your analysis',
    entry_points={
        'console_scripts': [f'{name}={name}.__main__:main']
    }
)

from setuptools import setup

setup(
    name='metrics_collector',
    version='0.0.1',
    packages=['metrics_collector'],
    install_requires=open('requirements.txt').read().split('\n'),
    url='',
    license='MIT',
    author='Daniel Engvall',
    author_email='daniel@engvalls.eu',
    description='Application for structuring your analysis',
    entry_points={
        'console_scripts': ['metrics_collector=metrics_collector.__main__:main']
    }
)

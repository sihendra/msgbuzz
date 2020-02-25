from setuptools import setup

setup(
    name='msgbus',
    version="0.1.0",
    author='Hendra Setiawan',
    author_email='sihendra@gmail.com',
    description='Generic message bus abstraction. Supported implementation: RabbitMQ through Pika',
    url='https://github.com/sihendra/msgbus',
    packages=['msgbus'],
    install_requires=open('requirements.txt', 'r').readlines(),
    include_package_data=True,
    long_description=open('README.md').read(),
)
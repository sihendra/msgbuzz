import setuptools

setuptools.setup(
    name='msgbus',
    version="0.0.1",
    author='Hendra Setiawan',
    author_email='sihendra@gmail.com',
    description='Generic message bus abstraction. Supported implementation: RabbitMQ through Pika',
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url='https://github.com/sihendra/msgbus',
    packages=setuptools.find_packages(),
    install_requires=open('requirements.txt', 'r').readlines(),
    python_requires='>=3.6'
)

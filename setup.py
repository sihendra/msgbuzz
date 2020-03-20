import setuptools

setuptools.setup(
    name="msgbuzz",
    version="0.1.0",
    author='Hendra Setiawan',
    author_email='sihendra@gmail.com',
    description='Generic message bus abstraction. Supported implementation: RabbitMQ through Pika',
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url='https://github.com/sihendra/msgbuzz',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    install_requires=open('requirements.txt', 'r').readlines(),
    python_requires='>=3.6'
)

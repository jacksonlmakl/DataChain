from setuptools import setup, find_packages
import pathlib

# Read the contents of your README file
here = pathlib.Path(__file__).parent.resolve()
long_description = (here / "README.md").read_text(encoding="utf-8")

setup(
    name='ChainEngine',
    version='2.2',
    packages=find_packages(),
    install_requires=[
        'requests',
        'pandas',
        'msgpack',
        'cryptography',
        
    ],
    author='Jackson Makl',
    author_email='jlm487@georgetown.edu',
    description='Decentralize data & processing using a blockchain on a peer to peer network.',
    long_description=long_description,
    long_description_content_type='text/markdown',  # Specify Markdown here
    url='https://github.com/jacksonlmakl/PyChain',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)

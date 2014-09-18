try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name="softheap",
    version='0.1',
    long_description=open('README.md').read(),
    url="http://github.com/URXtech/softheap",
    author="URX",
    author_email="opensource@urx.com",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    install_requires=[l.strip() for l in open('requirements.txt')],
    tests_require=['pytest'],
    package_data={'': ['storage_manager.h', 'libsoftheap.so', 'libck.so.0.4.3']},
    packages=['persistent_queue'],
    include_package_data=True,
    zip_safe=False
)

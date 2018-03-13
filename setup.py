from setuptools import setup, Command

setup(name='pybloomg',
      version="2.1.4",
      description='Client library to interface with bloomg server',
      author='Thomas Wang',
      author_email='thomas@kiip.me',
      maintainer='Thomas Wang',
      maintainer_email='thomas@kiip.me',
      license="MIT License",
      keywords=["bloom", "filter","client","bloomg"],
      packages=['pybloomg'],
      py_modules=['pybloomg'],
      classifiers=[
            "Development Status :: 4 - Beta",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: MIT License",
            "Operating System :: POSIX",
            "Programming Language :: Python",
            "Topic :: Database",
            "Topic :: Internet",
            "Topic :: Software Development :: Libraries",
      ]
      )
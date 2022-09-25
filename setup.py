from setuptools import setup


setup(
    name='bdtool',
    version="1.2.2",
    author="linusic",
    author_email='cython_lin@cklin.top',
    description="script of managing some bigdata technology stack",
    url="https://github.com/linusic/bdtool",   # Github Repositoriy (auto sync stars ...)
    classifiers=[
        'Intended Audience :: Developers',        
        "Environment :: Console",                 
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        "Topic :: Software Development",
        "Topic :: Utilities",
    ],  

    entry_points={ 
        "console_scripts": [ 
            "fa=bdtool:main" # bdtool.py -> def main
        ]
    },
    py_modules=["bdtool"], 
    python_requires='>=3.6',
)

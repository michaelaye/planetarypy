.. highlight:: shell

Contributing
============

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

For a high-level overview of the philosophy of contributions to PlanetaryPy,
which applies to this project, please see
https://github.com/planetarypy/TC/blob/master/Contributing.md.

You can contribute in many ways:

Types of Contributions
----------------------

Report Bugs
~~~~~~~~~~~

Report bugs at https://github.com/planetarypy/planetarypy/issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

Fix Bugs
~~~~~~~~

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

Implement Features
~~~~~~~~~~~~~~~~~~

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

Write Documentation
~~~~~~~~~~~~~~~~~~~

planetarypy could always use more documentation, whether as part of the
official planetarypy docs, in docstrings, or even on the web in blog posts,
articles, and such.

Submit Feedback
~~~~~~~~~~~~~~~

The best way to send feedback is to file an issue at https://github.com/planetarypy/planetarypy/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

Get Started!
------------

Ready to contribute? Here's how to set up `planetarypy` for local development.

1. Fork the `planetarypy` repo on GitHub.
2. Clone your fork locally::

    $ git clone git@github.com:your_name_here/planetarypy.git

3. Install your local copy into a virtualenv. Assuming you have virtualenvwrapper installed, this is how you set up your fork for local development::

    $ mkvirtualenv planetarypy
    $ cd planetarypy/
    $ pip install -e .

4. Create a branch for local development::

    $ git checkout -b name-of-your-bugfix-or-feature

   Now you can make your changes locally.

5. When you're done making changes, check that your changes pass flake8 and the
   tests, including testing other Python versions with tox::

    $ flake8 planetarypy tests
    $ python setup.py test or pytest
    $ tox

   To get flake8 and tox, just pip install them into your virtualenv.

6. Commit your changes and push your branch to GitHub::

    $ git add .
    $ git commit -m "Changes summary

        Your detailed description of your changes."
    $ git push origin name-of-your-bugfix-or-feature

7. Submit a pull request through the GitHub website.

Pull Request Guidelines
-----------------------

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests covering the changes.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.rst.
3. The pull request should minimally work for Python version 3.9.

The action protocol, specifically timeline and reviews for pull requests as described in
https://github.com/planetarypy/TC/blob/master/Contributing.md#contributions
is applicable here, with the following changes for the time being and to be reviewed later:

In order to merge a PR, it must satisfy **ONLY ONE** condition:

* have **ONE** approval

Also, for this early stage of filling up the core package, we add the special rule, that

* after three days without a review, the PR can be merged by the requester.

We emphasize that this is only done temporarily to support a quicker growth and this
procedure will be reviewed as soon as we feel that the core package either
has a significant number of users and/or that PRs break available functionality on a
regular basis, due to a lack of reviews.

Deploying
---------

A reminder for the maintainers on how to deploy.
Make sure all your changes are committed (including an entry in HISTORY.rst).
Then run::

$ bump2version patch # possible: major / minor / patch
$ git push
$ git push --tags

Deployment to pypi to be determined.

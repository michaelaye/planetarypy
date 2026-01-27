# Contributing

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

For a high-level overview of the philosophy of contributions to PlanetaryPy,
which applies to this project, please see
<https://github.com/planetarypy/TC/blob/master/Contributing.md>.

## Types of Contributions

- **Report Bugs**: <https://github.com/planetarypy/planetarypy/issues>
- **Fix Bugs**: Look through GitHub issues tagged "bug" and "help wanted"
- **Implement Features**: Look through GitHub issues tagged "enhancement" and "help wanted"
- **Write Documentation**: planetarypy could always use more documentation

## Get Started!

Ready to contribute? Here's how to set up `planetarypy` for local development.

1. Fork the `planetarypy` repo on GitHub.

2. Clone your fork locally:
   ```bash
   git clone git@github.com:your_name_here/planetarypy.git
   cd planetarypy/
   ```

3. Install dependencies (uses mamba/conda where possible):
   ```bash
   python install_dev_deps.py
   pip install -e .
   ```

4. Install Quarto for building documentation:
   ```bash
   # macOS
   brew install quarto

   # Other platforms: https://quarto.org/docs/get-started/
   ```

5. Create a branch for local development:
   ```bash
   git checkout -b name-of-your-bugfix-or-feature
   ```

6. Make your changes locally.

7. When you're done making changes, run the tests:
   ```bash
   pytest
   ```

8. Commit your changes and push your branch to GitHub:
   ```bash
   git add .
   git commit -m "Your detailed description of your changes."
   git push origin name-of-your-bugfix-or-feature
   ```

9. Submit a pull request through the GitHub website.

## Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests covering the changes.
2. If the pull request adds functionality, the docs should be updated.
3. The pull request should work for Python 3.11+.

The action protocol, specifically timeline and reviews for pull requests as described in
<https://github.com/planetarypy/TC/blob/master/Contributing.md#contributions>
is applicable here, with the following changes:

In order to merge a PR, it must have **ONE** approval.

Also, for this early stage, after three days without a review, the PR can be merged by the requester.

## Deploying (Maintainers)

A reminder for the maintainers on how to deploy.
Make sure all your changes are committed (including an entry in HISTORY.rst).
Then run:

```bash
bump2version patch  # possible: major / minor / patch
git push
git push --tags
```

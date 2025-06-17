---
applyTo: "**/*.py"
---

# GitHub Copilot Instructions

These instructions define how GitHub Copilot should assist with this project. The goal is to ensure consistent, high-quality code generation aligned with our conventions, stack, and best practices.

## 🧠 Context

- **Project Type**: Cluster Computing
- **Language**: Python
- **Framework / Libraries**: Redis / DRMAA / Numpy / Pandas
- **Environment**: The conda environment for this repository is called 'vct'. Please ensure the environment is activated when running terminal commands.
## 🔧 General Guidelines

- Use Pythonic patterns (PEP8, PEP257).
- Use numpydoc docstring styling as show below.
- Prefer named functions and class-based structures over inline lambdas.
- Use type hints.
- Follow black or isort for formatting and import order.
- Use meaningful naming; avoid cryptic variables.
- Emphasize simplicity, readability, and DRY principles.
- Make atomic, commit-level changes, then commit with a useful message.
- Use unit tests or short scripts to test that the code is working properly. if the acceptance criteria is not clear, ask the user.
- Ask questions and code collaboratively with the user. Make sure you don't make so many edits at once that the user cannot keep up.
- Always prefer to incorporate new code into existing modules over creating new files, especially in test code.
- You can use comments as a a placeholder or internal monologue, but unless they are critical to understanding a non-intuitive section of code,
they should be removed. In general, we shouldn't  have a bunch of unnecessary comments peppering the code. 

## 🧶 Patterns

### 🚫 Patterns to Avoid

- Don’t use wildcard imports (`from module import *`).
- Avoid global state unless encapsulated in a singleton or config manager.
- Don’t hardcode secrets or config values—use `.env`.
- Don’t expose internal stack traces in production environments.
- Avoid business logic inside views/routes.

## 🧪 Testing Guidelines

- Use `pytest` for unit and integration tests.
- Mock external services with  `pytest-mock`.
- Use fixtures to set up and tear down test data.
- Aim for high coverage on core logic and low-level utilities.
- Test both happy paths and edge cases.

## Docstring Style
- Use numpydoc style, example given below. However, on our team we do not add
types in the docstring if the callable is type-hinted (which it should be).

"""Docstring for the example.py module.

Modules names should have short, all-lowercase names.  The module name may
have underscores if this improves readability.

Every module should have a docstring at the very top of the file.  The
module's docstring may extend over multiple lines.  If your docstring does
extend over multiple lines, the closing three quotation marks must be on
a line by itself, preferably preceded by a blank line.

"""

import os  # standard library imports first

# Do NOT import using *, e.g. from numpy import *
#
# Import the module using
#
#   import numpy
#
# instead or import individual functions as needed, e.g
#
#  from numpy import array, zeros
#
# If you prefer the use of abbreviated module names, we suggest the
# convention used by NumPy itself::

import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

# These abbreviated names are not to be used in docstrings; users must
# be able to paste and execute docstrings after importing only the
# numpy module itself, unabbreviated.


def foo(var1: array_like, var2: int, *args: Any, long_var_name: str | None ="hi", only_seldom_used_keyword: int=0, **kwargs: Any) -> type:
    r"""Summarize the function in one line.

    Several sentences providing an extended description. Refer to
    variables using back-ticks, e.g. `var`.

    Parameters
    ----------
    var1
        Array_like means all those objects -- lists, nested lists, etc. --
        that can be converted to an array.  We can also refer to
        variables like `var1`.
    var2
        The type above can either refer to an actual Python type
        (e.g. ``int``), or describe the type of the variable in more
        detail, e.g. ``(N,) ndarray`` or ``array_like``.
    *args
        Other arguments.
    long_var_name
        Choices in brackets, default first when optional.

    Returns
    -------
    Explanation of anonymous return value of type ``type``.


    Other Parameters
    ----------------
    only_seldom_used_keyword
        Infrequently used parameters can be described under this optional
        section to prevent cluttering the Parameters section.
    **kwargs
        Other infrequently used keyword arguments. Note that all keyword
        arguments appearing after the first parameter specified under the
        Other Parameters section, should also be described under this
        section.

    Raises
    ------
    BadException
        Because you shouldn't have done that.

    See Also
    --------
    numpy.array : Relationship (optional).
    numpy.ndarray : Relationship (optional), which could be fairly long, in
                    which case the line wraps here.
    numpy.dot, numpy.linalg.norm, numpy.eye

    Notes
    -----
    Notes about the implementation algorithm (if needed).

    This can have multiple paragraphs.

    You may include some math:

    .. math:: X(e^{j\omega } ) = x(n)e^{ - j\omega n}

    And even use a Greek symbol like :math:`\omega` inline.

    References
    ----------
    Cite the relevant literature, e.g. [1]_.  You may also cite these
    references in the notes section above.

    .. [1] O. McNoleg, "The integration of GIS, remote sensing,
       expert systems and adaptive co-kriging for environmental habitat
       modelling of the Highland Haggis using object-oriented, fuzzy-logic
       and neural-network techniques," Computers & Geosciences, vol. 22,
       pp. 585-588, 1996.

    Examples
    --------
    These are written in doctest format, and should illustrate how to
    use the function.

    >>> a = [1, 2, 3]
    >>> print([x + 3 for x in a])
    [4, 5, 6]
    >>> print("a\nb")
    a
    b
    """
    # After closing class docstring, there should be one blank line to
    # separate following codes (according to PEP257).
    # But for function, method and module, there should be no blank lines
    # after closing the docstring.

## 📚 References

- [PEP 8 – Style Guide for Python Code](https://peps.python.org/pep-0008/)
- [PEP 484 – Type Hints](https://peps.python.org/pep-0484/)
- [Pytest Documentation](https://docs.pytest.org/en/stable/)
- [Python Logging Best Practices](https://docs.python.org/3/howto/logging.html)
- [Black Code Formatter](https://black.readthedocs.io/)
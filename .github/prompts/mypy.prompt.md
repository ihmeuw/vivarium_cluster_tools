---
mode: 'agent'
description: 'Static Type Checking'
---

Fix mypy errors in this repository in a systematic and iterative way. You should undertake the following workflow:

* Go to pyproject.toml and remove a single excluded file from the [tool.mypy] exclude list, starting below where it says:
    "Files below here should have their errors fixed and then be removed from this list"
    "You will need to remove the mypy: ignore-errors comment from the file heading as well"
only remove a single file at a time. 
* remove the corresponding mypy: ignore-errors comment from that file's heading

* Run `mypy .` from the repository's root directory. Fix all errors until mypy shows success.
* Do not change any business logic in the code. If you think there is a bug that requires a change to the business logic, leave a #TODO explaining why, and ignore the error.
* Any circumstance in which you ignore an error or use an Any type hint, leave a comment briefly explaining why it cannot be fixed in another way.
* After you have fixed all the errors in the file, commit the changes with a meaningful commit message, and then report back to the user.
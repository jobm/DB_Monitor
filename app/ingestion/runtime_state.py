"""Compatibility wrapper for moved consumer runtime state.

Consumer runtime internals now live under the consumer package.
Keep this module as a transitional import path for older references.
"""

from consumer.runtime_state import *  # noqa: F401,F403

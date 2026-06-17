import re
from typing import Set

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class SQSEvent(BaseModel):
    """Base class for SQS event models.

    Subclasses declare typed fields; the class name is the message type used for
    routing. Both snake_case field names and their camelCase aliases are accepted
    (Pydantic alias generation with ``populate_by_name``), so a payload may use
    either convention without bespoke normalization.
    """

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    @classmethod
    def get_message_type(cls) -> str:
        """Primary message type for this event class (snake_case of the class name)."""
        name = cls.__name__
        return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

    @classmethod
    def get_message_type_variants(cls) -> Set[str]:
        """Message-type variants for flexible matching: the class name plus its
        snake_case, camelCase and kebab-case forms."""
        base_name = cls.__name__
        if not base_name:
            return set()
        snake = re.sub(r'(?<!^)(?=[A-Z])', '_', base_name).lower()
        camel = base_name[0].lower() + base_name[1:]
        kebab = re.sub(r'(?<!^)(?=[A-Z])', '-', base_name).lower()
        return {base_name, snake, camel, kebab}

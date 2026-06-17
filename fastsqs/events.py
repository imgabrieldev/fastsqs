from pydantic import BaseModel, model_validator
from typing import Any, Dict, Set
import re


class SQSEvent(BaseModel):
    """Base class for SQS event models with automatic field normalization.
    
    This class provides automatic conversion between different naming conventions
    (camelCase, snake_case, kebab-case) and generates message type variants.
    """
    
    @model_validator(mode='before')
    @classmethod
    def normalize_field_names(cls, data: Any) -> Any:
        """Normalize field names from different naming conventions.
        
        Args:
            data: Input data dictionary
            
        Returns:
            Normalized data dictionary
        """
        if not isinstance(data, dict):
            return data
            
        model_fields = set()
        if hasattr(cls, 'model_fields'):
            model_fields = set(cls.model_fields.keys())
        
        normalized_data = dict(data)
        
        for field_name in model_fields:
            if field_name not in normalized_data:
                parts = field_name.split('_')
                if len(parts) > 1:
                    camel_case = parts[0] + ''.join(word.capitalize() for word in parts[1:])
                    if camel_case in normalized_data:
                        normalized_data[field_name] = normalized_data[camel_case]
                        continue
                
                field_normalized = field_name.lower().replace('_', '').replace('-', '')
                for key in data:
                    key_normalized = key.lower().replace('_', '').replace('-', '')
                    if key != field_name and key_normalized == field_normalized:
                        normalized_data[field_name] = normalized_data[key]
                        break
        
        return normalized_data
    
    @classmethod
    def get_message_type(cls) -> str:
        """Get the primary message type for this event class.
        
        Returns:
            Snake_case version of the class name
        """
        name = cls.__name__
        return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    
    @classmethod
    def get_message_type_variants(cls) -> Set[str]:
        """Get all possible message type variants for flexible matching.
        
        Returns:
            Set of message type variants in different naming conventions
        """
        base_name = cls.__name__
        
        variants = set()
        
        variants.add(base_name)
        
        snake_case = re.sub(r'(?<!^)(?=[A-Z])', '_', base_name).lower()
        variants.add(snake_case)
        
        camel_case = base_name[0].lower() + base_name[1:] if base_name else ""
        variants.add(camel_case)
        
        kebab_case = re.sub(r'(?<!^)(?=[A-Z])', '-', base_name).lower()
        variants.add(kebab_case)
        
        variants.add(base_name.lower())
        
        variants.add(base_name.upper())
        
        return variants
    
    @classmethod
    def from_sqs_record(cls, record: Dict[str, Any]) -> "SQSEvent":
        """Create an event instance from an SQS record.
        
        Args:
            record: SQS record dictionary
            
        Returns:
            Validated event instance
        """
        import json
        body = json.loads(record.get("body", "{}"))
        return cls.model_validate(body)

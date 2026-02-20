# input_handler.py - DUAL MODE INPUT HANDLER (CLI & UI)
import os
import sys

import logging
logger = logging.getLogger(__name__)

class InputHandler:
    """Handler for dual-mode input (CLI & UI)"""
    
    def __init__(self, mode='cli', data=None):
        """
        Initialize input handler
        
        Args:
            mode: 'cli' or 'ui'
            data: Dictionary containing UI input data for UI mode
        """
        self.mode = mode.lower()
        self.ui_data = data or {}
        self.current_ui_data = {}
        
    def set_ui_data(self, data):
        """Set UI data for UI mode"""
        self.ui_data = data or {}
        self.current_ui_data = {}
    
    def get_input(self, prompt, field_name=None, default=None, required=True):
        """
        Get input in dual mode - FIXED VERSION
        
        Args:
            prompt: Text to show in CLI mode
            field_name: Key name in UI data dictionary
            default: Default value if not provided
            required: Whether input is required
            
        Returns:
            Input value
        """
        if self.mode == 'ui':
            # UI Mode: Get from pre-provided data
            value = None
            
            # Try to get from current UI data first (for nested inputs)
            if field_name and field_name in self.current_ui_data:
                value = self.current_ui_data[field_name]
            
            # Then try from main UI data
            if value is None and field_name and field_name in self.ui_data:
                value = self.ui_data[field_name]
            
            # If still not found and there's a default, use it
            if value is None and default is not None:
                value = default
            
            # FIX: Instead of raising error, return empty string or default for UI mode
            # This allows the business rules to provide sensible defaults
            if required and value is None:
                # Return empty string instead of raising error
                logger.warning(f"Required field '{field_name}' not provided in UI mode, using empty string")
                return ""
            
            # Convert to string for consistency
            if value is not None:
                value = str(value).strip()
            
            return value if value is not None else ""
        
        else:
            # CLI Mode: Use input() as before
            if default is not None:
                prompt = f"{prompt} [default: {default}]: "
            else:
                prompt = f"{prompt}: "
            
            value = input(prompt).strip()
            
            # Use default if empty
            if not value and default is not None:
                value = default
            
            # If required and empty, ask again
            while required and not value:
                print(f"⚠️  This field is required!")
                value = input(prompt).strip()
                if not value and default is not None:
                    value = default
            
            return value
    
    def get_choice(self, prompt, options, field_name=None):
        """
        Get choice from options in dual mode
        
        Args:
            prompt: Text to show in CLI mode
            options: List of options or dict mapping choice to value
            field_name: Key name in UI data dictionary
            
        Returns:
            Selected choice
        """
        if self.mode == 'ui':
            # UI Mode: Get from pre-provided data
            if field_name and field_name in self.ui_data:
                choice = str(self.ui_data[field_name]).strip()
                
                # Validate choice
                if isinstance(options, list):
                    if choice in options or choice in [str(i) for i in range(1, len(options)+1)]:
                        return choice
                elif isinstance(options, dict):
                    if choice in options or choice in options.values():
                        return choice
                
                # If invalid, use first option as default
                if isinstance(options, list):
                    return str(options[0]) if options else ""
                elif isinstance(options, dict):
                    return str(list(options.keys())[0]) if options else ""
            
            # Default to first option
            if isinstance(options, list):
                return str(options[0]) if options else ""
            elif isinstance(options, dict):
                return str(list(options.keys())[0]) if options else ""
            else:
                return ""
        
        else:
            # CLI Mode: Show options and get input
            print(prompt)
            
            if isinstance(options, list):
                for i, option in enumerate(options, 1):
                    print(f"  {i}. {option}")
                
                while True:
                    choice = input(f"Enter choice (1-{len(options)}): ").strip()
                    if choice.isdigit() and 1 <= int(choice) <= len(options):
                        return options[int(choice)-1]
                    elif choice in options:
                        return choice
                    else:
                        print(f"❌ Invalid choice. Please enter 1-{len(options)} or option name.")
            
            elif isinstance(options, dict):
                for key, value in options.items():
                    print(f"  {key}. {value}")
                
                while True:
                    choice = input(f"Enter choice ({'/'.join(options.keys())}): ").strip()
                    if choice in options:
                        return choice
                    elif choice in options.values():
                        # Find key by value
                        for k, v in options.items():
                            if v == choice:
                                return k
                    else:
                        print(f"❌ Invalid choice. Please enter one of: {', '.join(options.keys())}")
    
    def get_multiple_choice(self, prompt, options, field_name=None):
        """
        Get multiple choice selection
        
        Args:
            prompt: Text to show in CLI mode
            options: List of options
            field_name: Key name in UI data dictionary
            
        Returns:
            List of selected options
        """
        if self.mode == 'ui':
            # UI Mode: Get from pre-provided data
            if field_name and field_name in self.ui_data:
                selections = self.ui_data[field_name]
                if isinstance(selections, str):
                    # Comma-separated string
                    return [s.strip() for s in selections.split(',') if s.strip()]
                elif isinstance(selections, list):
                    return [str(s).strip() for s in selections]
            return []
        
        else:
            # CLI Mode
            print(prompt)
            for i, option in enumerate(options, 1):
                print(f"  {i}. {option}")
            
            print("  Enter selections as comma-separated numbers (e.g., 1,3,5) or type 'all'")
            
            while True:
                choice = input("Enter your selections: ").strip()
                
                if choice.lower() == 'all':
                    return options
                
                try:
                    indices = [int(x.strip()) for x in choice.split(',')]
                    selected = []
                    for idx in indices:
                        if 1 <= idx <= len(options):
                            selected.append(options[idx-1])
                    
                    if selected:
                        return selected
                    else:
                        print("❌ No valid selections. Please try again.")
                except ValueError:
                    print("❌ Invalid input. Please enter comma-separated numbers or 'all'.")
    
    def set_current_data(self, data):
        """Set current UI data for nested input contexts"""
        self.current_ui_data = data or {}
    
    def clear_current_data(self):
        """Clear current UI data"""
        self.current_ui_data = {}

# Global input handler instance
_input_handler = None

def init_input_handler(mode='cli', data=None):
    """Initialize global input handler"""
    global _input_handler
    _input_handler = InputHandler(mode, data)
    return _input_handler

def get_input_handler():
    """Get global input handler"""
    global _input_handler
    if _input_handler is None:
        _input_handler = InputHandler()
    return _input_handler

def get_input(prompt, field_name=None, default=None, required=True):
    """Convenience function to get input"""
    handler = get_input_handler()
    return handler.get_input(prompt, field_name, default, required)

def get_choice(prompt, options, field_name=None):
    """Convenience function to get choice"""
    handler = get_input_handler()
    return handler.get_choice(prompt, options, field_name)

def get_multiple_choice(prompt, options, field_name=None):
    """Convenience function to get multiple choice"""
    handler = get_input_handler()
    return handler.get_multiple_choice(prompt, options, field_name)
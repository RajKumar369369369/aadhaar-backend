"""
DML Operations Generator
Generates Data Manipulation Language (INSERT, UPDATE, DELETE, SELECT) operations 
for the janasena.person table in the Aadhaar application.
"""

from datetime import date, datetime
from typing import Dict, List, Optional, Any
from models import Person
from database import SessionLocal, engine


class DMLGenerator:
    """Generate DML operations for the Person table."""

    def __init__(self):
        self.schema = "janasena"
        self.table = "person"
        self.session = SessionLocal()

    def generate_insert(self, person_data: Dict[str, Any]) -> str:
        """
        Generate an INSERT statement for a new person record.
        
        Args:
            person_data: Dictionary containing person information
            
        Returns:
            SQL INSERT statement as string
        """
        columns = list(person_data.keys())
        values = list(person_data.values())
        
        # Format values
        formatted_values = []
        for value in values:
            if value is None:
                formatted_values.append("NULL")
            elif isinstance(value, str):
                formatted_values.append(f"'{value}'")
            elif isinstance(value, (date, datetime)):
                formatted_values.append(f"'{value}'")
            elif isinstance(value, bool):
                formatted_values.append(str(value))
            else:
                formatted_values.append(str(value))
        
        columns_str = ", ".join(columns)
        values_str = ", ".join(formatted_values)
        
        return f"""INSERT INTO {self.schema}.{self.table} ({columns_str})
VALUES ({values_str});"""

    def generate_bulk_insert(self, records: List[Dict[str, Any]]) -> str:
        """
        Generate bulk INSERT statements for multiple records.
        
        Args:
            records: List of dictionaries containing person information
            
        Returns:
            SQL bulk INSERT statement
        """
        if not records:
            return ""
        
        columns = list(records[0].keys())
        columns_str = ", ".join(columns)
        
        values_list = []
        for record in records:
            formatted_values = []
            for key in columns:
                value = record.get(key)
                if value is None:
                    formatted_values.append("NULL")
                elif isinstance(value, str):
                    formatted_values.append(f"'{value}'")
                elif isinstance(value, (date, datetime)):
                    formatted_values.append(f"'{value}'")
                elif isinstance(value, bool):
                    formatted_values.append(str(value))
                else:
                    formatted_values.append(str(value))
            values_list.append(f"({', '.join(formatted_values)})")
        
        values_str = ",\n".join(values_list)
        
        return f"""INSERT INTO {self.schema}.{self.table} ({columns_str})
VALUES
{values_str};"""

    def generate_update(self, person_id: int, updates: Dict[str, Any]) -> str:
        """
        Generate an UPDATE statement for a person record.
        
        Args:
            person_id: ID of the person to update
            updates: Dictionary of fields and values to update
            
        Returns:
            SQL UPDATE statement
        """
        set_clauses = []
        for key, value in updates.items():
            if value is None:
                set_clauses.append(f"{key} = NULL")
            elif isinstance(value, str):
                set_clauses.append(f"{key} = '{value}'")
            elif isinstance(value, (date, datetime)):
                set_clauses.append(f"{key} = '{value}'")
            elif isinstance(value, bool):
                set_clauses.append(f"{key} = {str(value)}")
            else:
                set_clauses.append(f"{key} = {str(value)}")
        
        set_str = ", ".join(set_clauses)
        return f"""UPDATE {self.schema}.{self.table}
SET {set_str}
WHERE person_id = {person_id};"""

    def generate_delete(self, person_id: int) -> str:
        """
        Generate a DELETE statement for a person record.
        
        Args:
            person_id: ID of the person to delete
            
        Returns:
            SQL DELETE statement
        """
        return f"""DELETE FROM {self.schema}.{self.table}
WHERE person_id = {person_id};"""

    def generate_delete_by_aadhaar(self, aadhaar_number: str) -> str:
        """
        Generate a DELETE statement for a person by Aadhaar number.
        
        Args:
            aadhaar_number: Aadhaar number of the person to delete
            
        Returns:
            SQL DELETE statement
        """
        return f"""DELETE FROM {self.schema}.{self.table}
WHERE aadhaar_number = '{aadhaar_number}';"""

    def generate_select_all(self) -> str:
        """
        Generate a SELECT statement to retrieve all person records.
        
        Returns:
            SQL SELECT statement
        """
        return f"SELECT * FROM {self.schema}.{self.table};"

    def generate_select_by_id(self, person_id: int) -> str:
        """
        Generate a SELECT statement to retrieve a person by ID.
        
        Args:
            person_id: ID of the person to select
            
        Returns:
            SQL SELECT statement
        """
        return f"""SELECT * FROM {self.schema}.{self.table}
WHERE person_id = {person_id};"""

    def generate_select_by_aadhaar(self, aadhaar_number: str) -> str:
        """
        Generate a SELECT statement to retrieve a person by Aadhaar number.
        
        Args:
            aadhaar_number: Aadhaar number of the person to select
            
        Returns:
            SQL SELECT statement
        """
        return f"""SELECT * FROM {self.schema}.{self.table}
WHERE aadhaar_number = '{aadhaar_number}';"""

    def generate_select_by_mobile(self, mobile_number: str) -> str:
        """
        Generate a SELECT statement to retrieve a person by mobile number.
        
        Args:
            mobile_number: Mobile number of the person to select
            
        Returns:
            SQL SELECT statement
        """
        return f"""SELECT * FROM {self.schema}.{self.table}
WHERE mobile_number = '{mobile_number}';"""

    def generate_select_by_constituency(self, constituency: str) -> str:
        """
        Generate a SELECT statement to retrieve all persons in a constituency.
        
        Args:
            constituency: Constituency name
            
        Returns:
            SQL SELECT statement
        """
        return f"""SELECT * FROM {self.schema}.{self.table}
WHERE constituency = '{constituency}';"""

    def generate_select_by_mandal(self, mandal: str) -> str:
        """
        Generate a SELECT statement to retrieve all persons in a mandal.
        
        Args:
            mandal: Mandal name
            
        Returns:
            SQL SELECT statement
        """
        return f"""SELECT * FROM {self.schema}.{self.table}
WHERE mandal = '{mandal}';"""

    def generate_select_with_filters(self, filters: Dict[str, Any]) -> str:
        """
        Generate a SELECT statement with multiple WHERE conditions.
        
        Args:
            filters: Dictionary of field names and values to filter by
            
        Returns:
            SQL SELECT statement
        """
        where_clauses = []
        for key, value in filters.items():
            if value is None:
                where_clauses.append(f"{key} IS NULL")
            elif isinstance(value, str):
                where_clauses.append(f"{key} = '{value}'")
            else:
                where_clauses.append(f"{key} = {value}")
        
        where_str = " AND ".join(where_clauses)
        return f"""SELECT * FROM {self.schema}.{self.table}
WHERE {where_str};"""

    def generate_count_all(self) -> str:
        """
        Generate a SELECT COUNT statement to count all person records.
        
        Returns:
            SQL COUNT statement
        """
        return f"SELECT COUNT(*) as total_records FROM {self.schema}.{self.table};"

    def generate_count_by_constituency(self, constituency: str) -> str:
        """
        Generate a SELECT COUNT statement for a specific constituency.
        
        Args:
            constituency: Constituency name
            
        Returns:
            SQL COUNT statement
        """
        return f"""SELECT COUNT(*) as total_records FROM {self.schema}.{self.table}
WHERE constituency = '{constituency}';"""

    def close(self):
        """Close the database session."""
        self.session.close()


def example_usage():
    """Example usage of DML Generator."""
    generator = DMLGenerator()
    
    # Example 1: Insert a single record
    person_data = {
        "aadhaar_number": "123456789012",
        "full_name": "John Doe",
        "dob": "1990-01-15",
        "gender": "Male",
        "mobile_number": "9876543210",
        "pincode": "500001",
        "constituency": "Hyderabad",
        "mandal": "Hyderabad",
        "panchayathi": "NA",
        "village": "NA",
        "ward_number": "1",
        "latitude": 17.3850,
        "longitude": 78.4867,
        "education": "B.Tech",
        "profession": "Engineer",
        "religion": "Hindu",
        "reservation": "General",
        "caste": "NA",
        "membership": "Yes",
        "membership_id": "JSP001",
        "aadhaar_image_url": "http://example.com/aadhaar.jpg",
        "photo_url": "http://example.com/photo.jpg"
    }
    
    print("=" * 80)
    print("INSERT EXAMPLE")
    print("=" * 80)
    print(generator.generate_insert(person_data))
    print()
    
    # Example 2: Update a record
    print("=" * 80)
    print("UPDATE EXAMPLE")
    print("=" * 80)
    updates = {
        "full_name": "Jane Doe",
        "mobile_number": "9876543211",
        "profession": "Doctor"
    }
    print(generator.generate_update(1, updates))
    print()
    
    # Example 3: Delete a record
    print("=" * 80)
    print("DELETE EXAMPLE")
    print("=" * 80)
    print(generator.generate_delete(1))
    print()
    
    # Example 4: Select all
    print("=" * 80)
    print("SELECT ALL EXAMPLE")
    print("=" * 80)
    print(generator.generate_select_all())
    print()
    
    # Example 5: Select by Aadhaar
    print("=" * 80)
    print("SELECT BY AADHAAR EXAMPLE")
    print("=" * 80)
    print(generator.generate_select_by_aadhaar("123456789012"))
    print()
    
    # Example 6: Select with filters
    print("=" * 80)
    print("SELECT WITH FILTERS EXAMPLE")
    print("=" * 80)
    filters = {
        "constituency": "Hyderabad",
        "gender": "Male"
    }
    print(generator.generate_select_with_filters(filters))
    print()
    
    # Example 7: Count all records
    print("=" * 80)
    print("COUNT ALL EXAMPLE")
    print("=" * 80)
    print(generator.generate_count_all())
    print()
    
    # Example 8: Bulk insert
    print("=" * 80)
    print("BULK INSERT EXAMPLE")
    print("=" * 80)
    records = [
        {
            "aadhaar_number": "111111111111",
            "full_name": "Person One",
            "mobile_number": "9111111111",
            "pincode": "500001",
            "constituency": "Hyderabad"
        },
        {
            "aadhaar_number": "222222222222",
            "full_name": "Person Two",
            "mobile_number": "9222222222",
            "pincode": "500002",
            "constituency": "Secunderabad"
        }
    ]
    print(generator.generate_bulk_insert(records))
    print()
    
    generator.close()


if __name__ == "__main__":
    example_usage()

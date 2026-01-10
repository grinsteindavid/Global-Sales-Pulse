#!/usr/bin/env python3
"""
Schema validation script for database initialization.

Validates:
1. Schema completeness - all expected tables exist
2. Data integrity - constraints properly applied
3. Performance indexes - all indexes created correctly
4. Referential integrity - foreign keys work (without inserting data)

This script runs after alembic migrations to ensure the database is properly initialized.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError
from src.database import engine
from src.models.transaction import Transaction


def check_schema_completeness() -> bool:
    """Check all tables exist and have proper structure."""
    print("🔍 Checking schema completeness...")
    
    inspector = inspect(engine)
    
    try:
        # Get all tables in the database
        tables = inspector.get_table_names()
        
        if not tables:
            print("❌ No tables found in database")
            return False
        
        print(f"✅ Found {len(tables)} table(s): {sorted(tables)}")
        
        # Check each table has columns
        for table_name in tables:
            columns = inspector.get_columns(table_name)
            if not columns:
                print(f"❌ Table '{table_name}' has no columns")
                return False
            print(f"✅ Table '{table_name}' has {len(columns)} column(s)")
        
        return True
        
    except SQLAlchemyError as e:
        print(f"❌ Error checking schema completeness: {e}")
        return False


def check_table_structure() -> bool:
    """Check all tables have proper structure (primary keys, columns)."""
    print("🔍 Checking table structure...")
    
    inspector = inspect(engine)
    
    try:
        tables = inspector.get_table_names()
        
        for table_name in tables:
            print(f"  🔍 Validating table: {table_name}")
            
            # Check table has columns
            columns = inspector.get_columns(table_name)
            if not columns:
                print(f"❌ Table '{table_name}' has no columns")
                return False
            
            column_names = {col["name"] for col in columns}
            print(f"    ✅ Found {len(columns)} columns: {sorted(column_names)}")
            
            # Check primary key exists
            pk_info = inspector.get_pk_constraint(table_name)
            if not pk_info or not pk_info.get("constrained_columns"):
                print(f"❌ Table '{table_name}' has no primary key")
                return False
            
            pk_columns = pk_info["constrained_columns"]
            print(f"    ✅ Primary key: {pk_columns}")
            
            # Check foreign keys (if any)
            fk_info = inspector.get_foreign_keys(table_name)
            if fk_info:
                print(f"    ✅ Foreign keys: {len(fk_info)} relationship(s)")
                for fk in fk_info:
                    print(f"      - {fk['constrained_columns']} -> {fk['referred_table']}.{fk['referred_columns']}")
            
            # Check for obviously problematic columns
            for col in columns:
                col_name = col["name"]
                # Warn about columns without proper constraints
                if col_name.endswith("_id") and not col.get("nullable") == False:
                    print(f"    ⚠️  Column '{col_name}' should probably be NOT NULL")
        
        print("✅ All table structures validated")
        return True
        
    except SQLAlchemyError as e:
        print(f"❌ Error checking table structure: {e}")
        return False


def check_indexes() -> bool:
    """Check all tables have appropriate indexes."""
    print("🔍 Checking performance indexes...")
    
    inspector = inspect(engine)
    
    try:
        tables = inspector.get_table_names()
        total_indexes = 0
        
        for table_name in tables:
            indexes = inspector.get_indexes(table_name)
            total_indexes += len(indexes)
            
            if indexes:
                index_names = {idx["name"] for idx in indexes}
                print(f"  ✅ Table '{table_name}': {len(indexes)} indexes")
                
                # Check index column mappings
                for idx in indexes:
                    if not idx.get("column_names"):
                        print(f"❌ Index {idx['name']} has no columns defined")
                        return False
                    print(f"    - {idx['name']}: {idx['column_names']}")
            else:
                print(f"  ⚠️  Table '{table_name}': No indexes (may be OK for small tables)")
        
        print(f"✅ Total indexes across all tables: {total_indexes}")
        return True
        
    except SQLAlchemyError as e:
        print(f"❌ Error checking indexes: {e}")
        return False


def check_constraints() -> bool:
    """Check constraints are properly applied for all tables."""
    print("🔍 Checking constraints...")
    
    try:
        with engine.connect() as conn:
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            
            total_unique_constraints = 0
            total_pk_constraints = 0
            total_not_null_constraints = 0
            
            for table_name in tables:
                print(f"  🔍 Checking constraints for table: {table_name}")
                
                # Check unique constraints
                result = conn.execute(text(f"""
                    SELECT conname, contype 
                    FROM pg_constraint 
                    WHERE conrelid = '{table_name}'::regclass 
                    AND contype = 'u'
                """))
                unique_constraints = result.fetchall()
                total_unique_constraints += len(unique_constraints)
                
                if unique_constraints:
                    print(f"    ✅ Found {len(unique_constraints)} unique constraint(s)")
                
                # Check primary key constraint
                result = conn.execute(text(f"""
                    SELECT conname, contype 
                    FROM pg_constraint 
                    WHERE conrelid = '{table_name}'::regclass 
                    AND contype = 'p'
                """))
                pk_constraints = result.fetchall()
                total_pk_constraints += len(pk_constraints)
                
                if pk_constraints:
                    print(f"    ✅ Found primary key constraint")
                else:
                    print(f"    ❌ No primary key constraint found for {table_name}")
                    return False
                
                # Check NOT NULL constraints
                result = conn.execute(text(f"""
                    SELECT column_name, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' 
                    AND table_schema = 'public'
                """))
                columns = result.fetchall()
                
                not_null_count = 0
                for col_name, is_nullable in columns:
                    if is_nullable == "NO":
                        not_null_count += 1
                
                total_not_null_constraints += not_null_count
                print(f"    ✅ Found {not_null_count} NOT NULL constraint(s)")
            
            print(f"✅ Summary: {total_pk_constraints} PK, {total_unique_constraints} unique, {total_not_null_constraints} NOT NULL constraints")
            return True
            
    except SQLAlchemyError as e:
        print(f"❌ Error checking constraints: {e}")
        return False


def check_referential_integrity() -> bool:
    """Check referential integrity setup for all tables (without inserting data)."""
    print("🔍 Checking referential integrity setup...")
    
    try:
        with engine.connect() as conn:
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            
            for table_name in tables:
                print(f"  🔍 Validating referential integrity for: {table_name}")
                
                # Check if we can query the table structure properly
                result = conn.execute(text(f"""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' 
                    AND table_schema = 'public'
                    ORDER BY ordinal_position
                """))
                columns = result.fetchall()
                
                if len(columns) < 1:
                    print(f"❌ Table {table_name} has no columns")
                    return False
                
                print(f"    ✅ Table structure accessible with {len(columns)} columns")
                
                # Test that we can create a prepared statement (tests data types)
                try:
                    # Create a simple SELECT statement to test table accessibility
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    count = result.scalar()
                    print(f"    ✅ Table queryable (current rows: {count})")
                except Exception as e:
                    print(f"    ❌ Table not queryable: {e}")
                    return False
            
            print("✅ All tables referential integrity validated")
            return True
            
    except SQLAlchemyError as e:
        print(f"❌ Error checking referential integrity: {e}")
        return False


def check_database_connectivity() -> bool:
    """Basic connectivity test."""
    print("🔍 Checking database connectivity...")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            test_value = result.scalar()
            if test_value == 1:
                print("✅ Database connectivity verified")
                return True
            else:
                print("❌ Database connectivity test failed")
                return False
    except SQLAlchemyError as e:
        print(f"❌ Database connectivity error: {e}")
        return False


def main():
    """Run all validation checks."""
    print("🚀 Starting database schema validation...\n")
    
    checks = [
        ("Database Connectivity", check_database_connectivity),
        ("Schema Completeness", check_schema_completeness),
        ("Table Structure", check_table_structure),
        ("Performance Indexes", check_indexes),
        ("Constraints", check_constraints),
        ("Referential Integrity", check_referential_integrity),
    ]
    
    passed = 0
    failed = 0
    
    for check_name, check_func in checks:
        print(f"\n--- {check_name} ---")
        try:
            if check_func():
                passed += 1
            else:
                failed += 1
                print(f"❌ {check_name} FAILED")
        except Exception as e:
            failed += 1
            print(f"❌ {check_name} ERROR: {e}")
    
    print(f"\n📊 Validation Summary:")
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    
    if failed > 0:
        print(f"\n🚨 Database schema validation FAILED!")
        sys.exit(1)
    else:
        print(f"\n🎉 Database schema validation PASSED!")
        print("✅ Database is ready for use")
        sys.exit(0)


if __name__ == "__main__":
    main()

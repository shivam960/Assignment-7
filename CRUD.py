import os
import sys
from datetime import datetime
import psycopg2
import psycopg2.extras

def timestamp():
    """Return current timestamp for log prefixing."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(msg):
    """Print message with timestamp prefix."""
    print(f"[{timestamp()}] {msg}")

def get_conn_params():
    """Build connection parameters from environment variables."""
    return {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE", "postgres"),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", "postgres"),
    }

def get_conn():
    """Create and return a database connection."""
    return psycopg2.connect(**get_conn_params())

def init_db():
    """Initialize the students table if it doesn't exist."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS students(
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        log("Database initialized")

def create_student(name, email):
    """Insert a new student and return their ID."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO students(name, email) VALUES(%s, %s) RETURNING id",
            (name, email)
        )
        return cur.fetchone()[0]

def list_students():
    """Retrieve all students as a list of dictionaries."""
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT id, name, email, created_at FROM students ORDER BY id")
        return [dict(row) for row in cur.fetchall()]

def update_student(student_id, name=None, email=None):
    """Update student fields. Returns number of rows affected."""
    updates, values = [], []
    if name is not None:
        updates.append("name=%s")
        values.append(name)
    if email is not None:
        updates.append("email=%s")
        values.append(email)
    if not updates:
        return 0
    
    values.append(student_id)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"UPDATE students SET {', '.join(updates)} WHERE id=%s",
            values
        )
        return cur.rowcount

def delete_student(student_id):
    """Delete a student by ID. Returns number of rows affected."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM students WHERE id=%s", (student_id,))
        return cur.rowcount

def print_table(rows):
    """Print rows in a formatted table with timestamps."""
    if not rows:
        log("No records found")
        return
    
    # Calculate column widths
    cols = list(rows[0].keys())
    widths = {col: max(len(col), max(len(str(row[col])) for row in rows)) for col in cols}
    
    # Print header
    header = " | ".join(col.ljust(widths[col]) for col in cols)
    log(header)
    log("-" * len(header))
    
    # Print rows
    for row in rows:
        line = " | ".join(str(row[col]).ljust(widths[col]) for col in cols)
        log(line)

def show_menu():
    """Display the main menu."""
    log("PostgreSQL Student CRUD")
    log("1) Create  2) List  3) Update  4) Delete  5) Quit")

def handle_create():
    """Handle student creation."""
    name = input("Name: ").strip()
    email = input("Email: ").strip()
    try:
        student_id = create_student(name, email)
        log(f"Created student with ID={student_id}")
    except Exception as e:
        log(f"Create error: {e}")

def handle_list():
    """Handle listing all students."""
    try:
        rows = list_students()
        print_table(rows)
    except Exception as e:
        log(f"List error: {e}")

def handle_update():
    """Handle student update."""
    try:
        student_id = int(input("Student ID: ").strip())
    except ValueError:
        log("Invalid ID")
        return
    
    name = input("New name (blank to skip): ").strip() or None
    email = input("New email (blank to skip): ").strip() or None
    
    try:
        rows_affected = update_student(student_id, name, email)
        log(f"Updated {rows_affected} row(s)")
    except Exception as e:
        log(f"Update error: {e}")

def handle_delete():
    """Handle student deletion."""
    try:
        student_id = int(input("Student ID: ").strip())
    except ValueError:
        log("Invalid ID")
        return
    
    try:
        rows_affected = delete_student(student_id)
        log(f"Deleted {rows_affected} row(s)")
    except Exception as e:
        log(f"Delete error: {e}")

def main():
    """Main application loop."""
    try:
        init_db()
    except Exception as e:
        log(f"Initialization error: {e}")
        sys.exit(1)
    
    handlers = {
        "1": handle_create,
        "2": handle_list,
        "3": handle_update,
        "4": handle_delete,
    }
    
    while True:
        show_menu()
        choice = input("> ").strip()
        
        if choice == "5":
            log("Goodbye")
            break
        elif choice in handlers:
            handlers[choice]()
        else:
            log("Invalid option")

if __name__ == "__main__":
    main()
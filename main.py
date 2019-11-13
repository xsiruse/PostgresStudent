from pprint import pprint

import psycopg2 as pg

db = pg.connect("dbname=netology_db user=netology_user password=123")
cur = db.cursor()


def db_cur(call_db):
    def calling(*args, **kwargs):
        with db:
            cur.execute("begin")
            call_db(*args, **kwargs)
            cur.execute("commit")
    return calling


@db_cur
def create_db(table_name, **kwargs):  # создает таблицы
    cur.execute("DROP TABLE IF EXISTS %s;" % table_name)  # для воспроизводимости повторно
    cur.execute("CREATE TABLE %s (id SERIAL PRIMARY KEY NOT NULL);" % table_name)
    for key, val in kwargs.items():
        # print(key, val)
        cur.execute("""
        ALTER TABLE %s
        ADD COLUMN %s %s;
        """ % (table_name, key, val))


@db_cur
def get_students(course_id):  # возвращает студентов определенного курса
    cur.execute("select * from Student where course_id = '%s'" % course_id)
    return pprint({r for r in cur.fetchall()})


@db_cur
def add_students(course, students):  # создает студентов и записывает их на курс
    cur.execute("select name from Student")
    student_exist = [r[0] for r in cur.fetchall()]
    for current_student in students:
        if current_student['name'] not in student_exist:
            add_student(current_student)
    cur.execute("select name from Course")
    course_list = [r[0] for r in cur.fetchall()]
    if course not in course_list:
        cur.execute("insert into course (name) values ('%s')" % course)
    for s in students:
        cur.execute("""
        UPDATE Student
        SET course_id = (SELECT id FROM Course where name = '%s')
        WHERE name = '%s';
        """ % (course, s['name']))


def add_student(student):  # просто создает студента
    cur.execute("""
    INSERT INTO Student (name, gpa, birth)
    VALUES ('%s', '%s', '%s');
    """ % (student['name'], student['gpa'], student['birth']))


@db_cur
def get_student(student_id):
    cur.execute("select * from Student where id = '%s'" %student_id)
    return print({r for r in cur.fetchall()})


def test_db_con():
    try:
        print(db.get_dsn_parameters(), "\n")
        cur.execute("SELECT version();")
        record = cur.fetchone()
        print("You are connected to - ", record, "\n")

    except (Exception, pg.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if db:
            cur.close()
            db.close()
            print("PostgreSQL connection is closed")


def main():
    cur.execute("DROP TABLE IF EXISTS Student;")  # для воспроизводимости повторно
    create_db('Course', name='character varying(100) not null')
    create_db('Student',
              name='character varying(100) not null',
              gpa='numeric(10,2)',
              birth='timestamp with time zone',
              course_id='integer REFERENCES Course(id)')
    students_dict = [
        {'name': 'Sergey Sergeev', 'gpa': '5', 'birth': '19.03.1998'},
        {'name': 'Mikhail Mikhailov', 'gpa': '5', 'birth': '19.05.1998'},
        {'name': 'Ramil Gulagov', 'gpa': '4', 'birth': '19.03.1997'},
        {'name': 'Nikolay Romanov', 'gpa': '2', 'birth': '14.03.1998'}
    ]

    add_students('Python advance', students_dict)
    add_students('Holly cow followers', students_dict)
    get_student(4)
    get_students(2)


if __name__ == '__main__':
    # test_db_con()
    main()

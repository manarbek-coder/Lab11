import psycopg2
from config import load_config


def collecting_info():
    """Извлекать данные из таблицы поставщиков"""
    config = load_config()
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, name, phone_number FROM phonebook ORDER BY user_id")
                rows = cur.fetchall()

                print("The number of parts : ", cur.rowcount)
                for row in rows:
                    print(row)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def update_info(user_id, name, phone_number):
    """Обновление контакта"""
    update_row_count = 0

    sql = """UPDATE phonebook
             SET name = %s, phone_number = %s
             WHERE user_id = %s"""

    config = load_config()

    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # Выполнение SQL-запроса UPDATE
                cur.execute(sql, (name, phone_number, user_id))
                update_row_count = cur.rowcount

            # Подтверждение изменений в базе данных
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        return update_row_count


def delete_info(user_id):
    """Удаление контакта"""
    rows_deleted = 0
    sql = 'DELETE FROM phonebook WHERE user_id = %s'
    config = load_config()

    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # execute the UPDATE statement
                cur.execute(sql, (user_id,))
                rows_deleted = cur.rowcount

            # commit the changes to the database
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        return rows_deleted


def update_or_insert_contact(name, phone_number):
    """Обновление номера телефона существующего контакта или вставка нового контакта"""
    config = load_config()
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # Проверяем существует ли контакт с таким именем
                cur.execute("SELECT user_id FROM phonebook WHERE name = %s", (name,))
                existing_contact = cur.fetchone()
                if existing_contact:
                    # Если контакт существует, обновляем его номер телефона
                    cur.execute("UPDATE phonebook SET phone_number = %s WHERE name = %s", (phone_number, name))
                else:
                    # Если контакт не существует, вставляем новый контакт
                    cur.execute("INSERT INTO phonebook (name, phone_number) VALUES (%s, %s) RETURNING user_id",
                                (name, phone_number))
                    user_id = cur.fetchone()[0]
                    return user_id  # Возвращаем идентификатор нового пользователя
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def search_records(pattern):
    """Поиск записей по шаблону"""
    config = load_config()
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, name, phone_number FROM phonebook WHERE name LIKE %s OR phone_number LIKE %s",
                            (f'%{pattern}%', f'%{pattern}%'))
                rows = cur.fetchall()

                print("Matching records:")
                for row in rows:
                    print(row)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def insert_or_update_user(name, phone_number):
    """Вставка нового пользователя по имени и телефону, обновление телефона, если пользователь уже существует"""
    config = load_config()
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # Проверяем существует ли контакт с таким именем
                cur.execute("SELECT user_id FROM phonebook WHERE name = %s", (name,))
                existing_contact = cur.fetchone()
                if existing_contact:
                    # Если контакт существует, обновляем его номер телефона
                    cur.execute("UPDATE phonebook SET phone_number = %s WHERE name = %s", (phone_number, name))
                    print("Контакт успешно обновлен.")
                else:
                    # Если контакт не существует, вставляем новый контакт
                    cur.execute("INSERT INTO phonebook (name, phone_number) VALUES (%s, %s)", (name, phone_number))
                    print("Новый контакт успешно добавлен.")
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def insert_many_users(users):
    """Вставка нескольких новых пользователей по списку имени и телефона"""
    config = load_config()
    incorrect_data = []
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                for user in users:
                    name, phone_number = user
                    if len(str(phone_number)) != 10:  # Checking correctness of phone number
                        incorrect_data.append(user)
                    else:
                        cur.execute("INSERT INTO phonebook (name, phone_number) VALUES (%s, %s)", (name, phone_number))
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        return incorrect_data


def query_with_pagination(limit, offset):
    """Запрос данных из таблиц с пагинацией (по лимиту и смещению)"""
    config = load_config()
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, name, phone_number FROM phonebook LIMIT %s OFFSET %s", (limit, offset))
                rows = cur.fetchall()

                print("Paginated records:")
                for row in rows:
                    print(row)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def delete_by_username_or_phone(pattern):
    """Удаление данных из таблиц по имени пользователя или номеру телефона"""
    config = load_config()
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM phonebook WHERE name = %s OR phone_number = %s", (pattern, pattern))
                conn.commit()
                print("Успешно удалено.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


if __name__ == '__main__':
    operation = input("Выберите\n1 - записать контакт\n2 - обновить контакт\n3 - пройтись по всем контактам\n4 - удаление контакта\n"
                      "5 - поиск записей\n6 - вставка или обновление пользователя\n7 - вставка нескольких пользователей\n"
                      "8 - запрос данных с пагинацией\n9 - удаление данных по имени пользователя или номеру телефона\n")

    if operation == "1":
        name = input("Введите имя нового контакта : ")
        phone_number = int(input("Введите номер телефон : "))
        user_id = update_or_insert_contact(name, phone_number)  # Пример вызова функции
        if user_id is not None:
            print("Контакт успешно добавлен или обновлен. ID:", user_id)
        else:
            print("Ошибка при добавлении контакта.")
    elif operation == "2":
        user_id = input("Введите айди контакта, который хотите обновить : ")
        name = input("Введите новое имя для контакта : ")
        phone_number = int(input("Введите новый номер телефона : "))
        updt = update_info(user_id, name, phone_number)
        if updt is not None:
            print("Контакт успешно изменен")
        else:
            print("Ошибка при изменениях")
    elif operation == "3":
        clct = collecting_info()
    elif operation == "4":
        user_id = int(input("Введите айди контакта, который хотите удалить : "))
        dlt = delete_info(user_id)
        if dlt is not None:
            print("Контакт успешно удален")
        else:
            print("Ошибка при удалениях")
    elif operation == "5":
        pattern = input("Введите шаблон для поиска записей: ")
        search_records(pattern)
    elif operation == "6":
        name = input("Введите имя пользователя: ")
        phone_number = input("Введите номер телефона пользователя: ")
        insert_or_update_user(name, phone_number)
    elif operation == "7":
        users = []
        n = int(input("Введите количество пользователей: "))
        for i in range(n):
            name = input("Введите имя пользователя: ")
            phone_number = input("Введите номер телефона пользователя: ")
            users.append((name, phone_number))
        incorrect_data = insert_many_users(users)
        if incorrect_data:
            print("Следующие данные были введены некорректно:", incorrect_data)
    elif operation == "8":
        limit = int(input("Введите лимит записей: "))
        offset = int(input("Введите смещение: "))
        query_with_pagination(limit, offset)
    elif operation == "9":
        pattern = input("Введите имя пользователя или номер телефона для удаления: ")
        delete_by_username_or_phone(pattern)

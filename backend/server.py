import jwt
import json
import bcrypt
import psycopg2
from flask import Flask, request
from traceback import format_exc
from datetime import datetime, timedelta

'''
PostgreSQL Database config:
    database='AIM', 
    user='postgres', 
    password='rf86yl97i', 
    host='localhost', 
    port='5432'

SQL query to create the table:
    CREATE TABLE users(
        user_id     SERIAL PRIMARY KEY,
        role        VARCHAR(100),
        first_name  VARCHAR(100),
        last_name   VARCHAR(100),
        email       VARCHAR(100) NOT NULL,
        password    CHAR(60) NOT NULL,
        last_login  DATE
    );
'''

class Server:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect_DB(self):
        self.connection = psycopg2.connect(
            database='AIM', 
            user='postgres', 
            password='rf86yl97i', 
            host='localhost', 
            port='5432'
        )
        self.cursor = self.connection.cursor()

    def disconnect_DB(self):
        self.connection.close()
    
    def hash_password(self, password=''):
        '''
        Hash a plain-text password
        return: hashed password string
        '''
        return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    def verify_password(self, email='', password=''):
        '''
        Check if the plain-text password matches the password in database.
        return: True if password is correct
        return: False if password is incorrect
        return: exception_message (str) if email does not exist
        '''
        self.cursor.execute(f"select password from users where email='{email}'")
        response = self.cursor.fetchall()
        if response:
            password_in_db = response[-1][0]
            return bcrypt.checkpw(password.encode('utf-8'), password_in_db.encode('utf-8')) 
        else:
            return f"Email: {email} does not exist. "

    def login(self, email='', password=''):
        '''
        Log in to an account
        return: user_data_dict if login was successful
        return: exception_message (str) if login failed
        '''
        try:
            verify_result = self.verify_password(email, password)
            if verify_result is True:
                query = f"select user_id, role, first_name, last_name from users where email='{email}'"
                self.cursor.execute(query)
                id, role, first_name, last_name = self.cursor.fetchall()[-1]
                data = {'id': id, 'role': role, 'firstName': first_name, 'lastName': last_name}
                today = datetime.now().strftime("%b-%d-%Y")
                self.cursor.execute(f"update users set last_login='{today}' where email='{email}'")
                self.connection.commit()
                return data
            if verify_result is False:
                return "Incorrect password. "
            return verify_result
        except:
            return format_exc()

    def create_user_data(self, role='', first_name='', last_name='', email='', password=''):
        '''
        Insert a row in users table
        return: True if the insertion was successful
        return: exception_message (str) if the insertion failed
        '''
        try:
            if self.check_email_exists(email):
                return f"Email: {email} already exists. "
            example_password_hash = self.hash_password(password)
            today = datetime.now().strftime("%b-%d-%Y")
            row = (role, first_name, last_name, email, example_password_hash, today)
            query = f"INSERT INTO users (role, first_name, last_name, email, password, last_login) VALUES {row}"
            self.cursor.execute(query)
            self.connection.commit()
            print(f"Email: {email} inserted")
            return True
        except:
            return format_exc()

    def check_email_exists(self, email):
        '''
        Check if an email exists in database. 
        return: True if the email exists. 
        return: False if the email doesn't exist. 
        '''
        self.cursor.execute(f"select email from users where email='{email}'")
        if self.cursor.fetchall():
            return True
        return False

    def change_password(self, email, old_password, new_password):
        '''
        Change password
        return: True if the updating was successful
        return: exception_message (str) if the updating failed
        '''
        try:
            if self.check_email_exists(email) is False:
                return f"Email: {email} does not exist."
            if old_password == new_password:
                return "New password is the same as old password. "
            verify_result = self.verify_password(email, old_password)
            if verify_result is True:
                new_password_str = self.hash_password(new_password)
                self.cursor.execute(f"update users set password='{new_password_str}' where email='{email}'")
                self.connection.commit()
                print(f"Password for {email} updated successfully.")
                return True
            if verify_result is False:
                return "Incorrect password. "
            return verify_result
        except:
            return format_exc()

    def delete_data(self, email, password):
        '''
        Change password
        return: True if the deleting was successful
        return: exception_message (str) if the deleting failed
        '''
        try:
            if self.check_email_exists(email) is False:
                return f"Email: {email} does not exist."
            verify_result = self.verify_password(email, password)
            if verify_result is True:
                self.cursor.execute(f"delete from users where email='{email}'")
                self.connection.commit()
                print(f"Deleted the account of {email} successfully ")
                return True
            if verify_result is False:
                return "Incorrect password. "
            return verify_result
        except:
            return format_exc()
    
    def get_user_id(self, email):
        '''get user_id by email'''
        try:
            self.cursor.execute(f"select user_id from users where email='{email}'")
            response = self.cursor.fetchall()
            if response:
                return response[-1][0]
            else:
                return f"Email: {email} does not exist."
        except:
            return format_exc()

    def generate_token(self, email):
        """
        Generates the Auth Token
        :return: string
        """
        try:
            utc_time_now = datetime.utcnow()
            user_id_response = self.get_user_id(email)
            if isinstance(user_id_response, int):
                payload = {
                    'exp': utc_time_now + timedelta(hours=2),
                    'iat': utc_time_now,
                    'sub': user_id_response
                }
                secret_key = email
                return jwt.encode(
                    payload,
                    secret_key,
                    algorithm='HS256'
                )
            else:
                return user_id_response
        except:
            return format_exc()

server = Server()
app = Flask(__name__)

@app.route('/api/auth/create', methods=['POST'])
def create():
    '''create an account'''
    result = {'message': '', 'token': ''}
    try:
        email = request.form['email']
        first_name = request.form['firstName']
        last_name = request.form['lastName']
        password = request.form['password']
        server.connect_DB()
        message = server.create_user_data(
            first_name = first_name, 
            last_name = last_name, 
            email = email, 
            password = password
        )
        if message is True:
            result['message'] = 'success'
            result['token'] = server.generate_token(email)
        else:
            result['message'] = message
    except:
        result['message'] = format_exc()
    finally:
        server.disconnect_DB()
        return json.dumps(result)
    
@app.route('/api/auth/login', methods=['POST'])
def login():
    '''login to an account'''
    result = {
        'message': '',
        'token': '',
        'data': {
            'id':'',
            'role':'',
            'firstName':'',
            'lastName':''
        }
    }
    try:
        email = request.form['email']
        password = request.form['password']
        server.connect_DB()
        message = server.login(
            email = email, 
            password = password
        )
        if isinstance(message, dict):
            result['message'] = 'success'
            result['token'] = server.generate_token(email)
            result['data'] = message
        else:
            result['message'] = message
    except:
        result['message'] = format_exc()
    finally:
        server.disconnect_DB()
        return json.dumps(result)

@app.route('/api/auth/change_password', methods=['POST'])
def change_password():
    result = {'message': ''}
    try:
        email = request.form['email']
        old_password = request.form['old_password']
        new_password = request.form['new_password']
        server.connect_DB()
        message = server.change_password(email, old_password, new_password)
        if message is True:
            result['message'] = 'success'
        else:
            result['message'] = message
    except:
        result['message'] = format_exc()
    finally:
        server.disconnect_DB()
        return json.dumps(result)

@app.route('/api/auth/delete', methods=['POST'])
def delete_account():
    result = {'message': ''}
    try:
        email = request.form['email']
        password = request.form['password']
        server.connect_DB()
        message = server.delete_data(email, password)
        if message is True:
            result['message'] = 'success'
        else:
            result['message'] = message
    except:
        result['message'] = format_exc()
    finally:
        server.disconnect_DB()
        return json.dumps(result)

if __name__ == '__main__':
    app.run(host='localhost', port=8000)

from collections import defaultdict
from datetime import datetime, timedelta
import random
import mysql.connector

# Ustawienia połączenia
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="12345678",
  database="sakila"
)

# Utwórz kursor
mycursor = mydb.cursor()

def fill_cinema_table():
    cinema_data = [
        (None, 'Kino 1', 5, 1),  
        (None, 'Kino 2', 3, 2),  
    ]
    # Zapytanie SQL
    sql = "INSERT INTO cinema (cinema_id, cinema_name, num_of_rooms, store_id) VALUES (%s, %s, %s, %s)"
    # Wstawianie danych do tabeli cinema
    mycursor.executemany(sql, cinema_data)
    # Zatwierdź transakcję
    mydb.commit()
    # Zamknij kursor
    mycursor.close()
    # Zamknij połączenie
    mydb.close()
    
    
def fill_room_table():
    room_data = [
        (None, 1, 'Sala 1', '2D', '00:20:00', 10, 10),  
        (None, 1, 'Sala 2', '3D', '00:20:00', 12, 10),  
        (None, 1, 'Sala 3', '2D', '00:10:00', 8, 15),  
        (None, 1, 'Sala 4', '3D', '00:20:00', 15, 8),  
        (None, 1, 'Sala 5', '2D', '00:10:00', 10, 12),  
        (None, 2, 'Sala 1', '2D', '00:10:00', 12, 8),  
        (None, 2, 'Sala 2', '3D', '00:30:00', 11, 15),  
        (None, 2, 'Sala 3', '2D', '00:15:00', 9, 14),  
        (None, 2, 'Sala 4', '3D', '00:30:00', 13, 12),  
        (None, 2, 'Sala 5', '2D', '00:10:00', 14, 10),
        (None, 2, 'Sala 6', '2D', '00:10:00', 14, 10), 
    ]
    valid_rooms_data = []  # Lista przechowująca dane tylko dla sal spełniających warunek
    for room in room_data:
        num_rows = room[5]
        seats_per_row = room[6]
        capacity = num_rows * seats_per_row  #  pojemność sali
        if capacity >= 100:
            valid_room = room[:3] + (capacity,) + room[3:]  #
            valid_rooms_data.append(valid_room) 
    for room in valid_rooms_data:
     print(len(room))
    if valid_rooms_data:
        sql = "INSERT INTO room (room_id, cinema_id, screen_name, capacity, type, cleaning_time, num_rows, seats_per_row) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        mycursor.executemany(sql, valid_rooms_data)
        mydb.commit()
    else:
        print("Nie dodano żadnej sali do bazy danych, ponieważ żadna z sal nie spełnia warunku pojemności większej niż 100.")

    mycursor.close()
    mydb.close()
    
def fill_seat_table():
    # pobierz wszystkie sale z bazy danych
    mycursor.execute("SELECT room_id, num_rows, seats_per_row FROM room")
    rooms = mycursor.fetchall()

    seat_data = []
    for room in rooms:
        room_id = room[0]
        num_rows = room[1]
        seats_per_row = room[2]

        # numery rzędów i miejsc w rzędach
        for row in range(1, num_rows + 1):
            for seat in range(1, seats_per_row + 1):
                # Losowo wybierz typ miejsca
                seat_type = random.choice(['VIP', 'NORMAL', 'DIS'])
                # Dla każdego miejsca dodaj dane do listy
                seat_data.append((None, row, seat, seat_type, room_id))
    sql = "INSERT INTO seat (seat_id, num_row, num_seat, type, room_id) VALUES (%s, %s, %s, %s, %s)"
    mycursor.executemany(sql, seat_data)

    mydb.commit()
    mycursor.close()
    mydb.close()

def fill_showing_table():
    try:
        mycursor.execute("SELECT room_id FROM room")
        rooms = mycursor.fetchall()

        # wszystkich film_id z bazy danych Sakila
        mycursor.execute("SELECT film_id FROM film")
        films = mycursor.fetchall()

        # Data początkowa
        start_date = datetime.now().date()
        # Tworzenie projekcji dla każdej sali
        for room_id in rooms:
            room_id = room_id[0]
            showings_per_day = defaultdict(int)
            for _ in range(10):
                # Tworzenie 10 projekcji dla każdej sali
                for _ in range(5):
                    # Losowanie daty z zakresu 10 dni od start_date
                    date = start_date + timedelta(days=random.randint(0, 10))
                    # Losowa godzina i czas trwania reklam
                    time = timedelta(hours=random.randint(9, 22), minutes=random.randint(0, 59))
                    ad_time = timedelta(minutes=random.randint(10, 30))
                    
                    # Losowe wybranie film_id z pobranych wcześniej filmów
                    film_id = random.choice(films)[0]
                    film_type = random.choice(['2D', '3D'])
                    
                    # Sprawdzanie, czy w danym dniu jest już wystarczająco projekcji
                    if showings_per_day[date] < 5:
                        showings_per_day[date] += 1
                        # Wstawianie danych do tabeli showing
                        sql = "INSERT INTO showing (film_id, room_id, date, time, ad_time, type) VALUES (%s, %s, %s, %s, %s, %s)"
                        mycursor.execute(sql, (film_id, room_id, date, time, ad_time, film_type))

        mydb.commit()

    except mysql.connector.Error as err:
        print("Error:", err)
        print("Skipping current iteration and continuing...")
        

    finally:
        mycursor.close()
        mydb.close()

def fill_booking_table():
    mycursor.execute("SELECT show_id FROM showing")
    shows = mycursor.fetchall()
    mycursor.execute("SELECT customer_id FROM customer")
    customers = mycursor.fetchall()

    # Pętla dla każdej projekcji
    for show_id in shows:
        show_id = show_id[0]
        # Tworzenie przynajmniej 20 rezerwacji dla każdej projekcji
        for _ in range(20):
            booking_date = datetime.now() - timedelta(days=random.randint(1, 365))
            num_tickets = random.randint(1, 5)
            customer_id = random.choice(customers)[0]
            sql = "INSERT INTO booking (customer_id, show_id, booking_date) VALUES (%s, %s, %s)"
            mycursor.execute(sql, (customer_id, show_id, booking_date))
            booking_id = mycursor.lastrowid

            # Pobranie wszystkich miejsc dla danej sali, które są wolne dla danej projekcji
            mycursor.execute("""SELECT seat_id 
                                FROM seat 
                                WHERE room_id = (SELECT room_id FROM showing WHERE show_id = %s) 
                                AND seat_id NOT IN 
                                    (SELECT seat_id 
                                    FROM ticket 
                                    WHERE show_id = %s)""", (show_id, show_id))
            available_seats = mycursor.fetchall()
            if len(available_seats) < num_tickets:
                print(f"Niewystarczająca liczba wolnych miejsc dla rezerwacji na projekcji {show_id}. Pomijanie rezerwacji.")
                continue
            chosen_seats = random.sample(available_seats, num_tickets)
            for seat in chosen_seats:
                seat_id = seat[0]
                sql = "INSERT INTO ticket (booking_id, seat_number, price, seat_id, show_id) VALUES (%s, %s, %s, %s, %s)"
                mycursor.execute(sql, (booking_id, seat_id, 10, seat_id, show_id))

    mydb.commit()
    mycursor.close()
    mydb.close()
    
def fill_tickets_table():

    mycursor.execute("SELECT show_id, room_id FROM showing")
    shows = mycursor.fetchall()

    for show in shows:
        show_id, room_id = show
        mycursor.execute("SELECT seat_id, type FROM seat WHERE room_id = %s", (room_id,))
        seats = mycursor.fetchall()

        for seat in seats:
            seat_id, seat_type = seat
            if seat_type == 'VIP':
                price = 40  # Cena biletu VIP
            elif seat_type == 'DIS':
                price = 15  # Cena biletu DIS
            else:
                price = 25  # Domyślna cena biletu

            sql = "INSERT INTO ticket (booking_id, price, seat_id, show_id, type_seat, status) VALUES (NULL, %s, %s, %s, %s, 'free')"
            val = (price/1.0, seat_id, show_id, seat_type)
            mycursor.execute(sql, val)
            mydb.commit()


def fill_booking_table():
    mycursor.execute("SELECT show_id FROM showing")
    shows = mycursor.fetchall()

    for show in shows:
        show_id = show[0]
        for _ in range(20):
            customer_id = random.randint(1, 550)

            mycursor.execute("SELECT seat_id FROM ticket WHERE show_id = %s AND status = 'free'", (show_id,))
            available_seats = mycursor.fetchall()

            if available_seats:
                random_seat_id = random.choice(available_seats)[0]

                sql = "INSERT INTO booking (customer_id, show_id, booking_date) VALUES (%s, %s, NOW())"
                val = (customer_id, show_id)
                mycursor.execute(sql, val)
                mydb.commit()

                booking_id = mycursor.lastrowid

                mycursor.execute("UPDATE ticket SET status = 'sold', booking_id = %s WHERE seat_id = %s AND show_id = %s AND status = 'free'", (booking_id, random_seat_id, show_id))
                mydb.commit()

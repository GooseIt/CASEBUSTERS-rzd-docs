import socket
import sys


s = socket.socket()
s.bind(("127.0.0.3", 9999))
s.listen(10)

i, count = 1, 0
while True:
    sc, address = s.accept()

    f = open(f"./upload/file_{i}.pdf", "wb")
    i += 1
    l = sc.recv(1024)
    new_try = True
    while l:
        if new_try:
            count += 1
            new_try = False

        f.write(l)
        l = sc.recv(1024)
    f.close()

    sc.close()
    if count > 1:
        break

s.close()

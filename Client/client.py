import PySimpleGUI as sg
import socket
import sys

sg.theme('Light Blue 2')

layout = [[sg.Text('Load Documents')],
          [sg.Text('File', size=(8, 1)), sg.Input(key='Input'), sg.FileBrowse()],
          [sg.Submit(), sg.Exit()]]

window = sg.Window('Check Document', layout)

count = 0
while True:
    if count > 1:
        break
    event, values = window.read()
    file = values['Input']
    if event == 'Exit' and file == "":
        break
    elif event in (sg.WINDOW_CLOSED, "Exit"):
        sys.exit()
    elif event == 'Submit':
        if '' != file:
            count += 1
            s = socket.socket()
            s.connect(("127.0.0.3",9999))
            f = open (file, "rb")
            l = f.read(1024)
            while (l):
                s.send(l)
                l = f.read(1024)
            s.close()

if values['Input'] == "":
    sys.exit()

window.close()

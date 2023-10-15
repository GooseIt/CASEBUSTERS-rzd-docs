import PySimpleGUI as sg

sg.theme("Light Blue 2")

accept = open("./Results/accept.txt", "r").read()
log = open("./Results/log.txt", "r").read()
statistics = open("./Results/statistics.txt", "r").read()

mistakes = "Ошибки:"
stat = "Статистики:"

layout = [
    [sg.Text(accept)],
    [sg.Text(mistakes)],
    [sg.Text(log)],
    [sg.Text(stat)],
    [sg.Text(statistics)],
    [sg.Exit()]
]

window = sg.Window("Results", layout)

while True:
    event, values = window.read()
    if event == "Exit":
        break

window.close()

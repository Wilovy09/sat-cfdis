import pyautogui
import time

pyautogui.FAILSAFE = True

pyautogui.PAUSE = 0.2

# Mejor si no tiene acentos, ya que puede generar errores en la escritura de los títulos de las tareas
TASKS = [
    "Mejora de trazabilidad, auditoría y diagnóstico de errores operativos",
    "Refinamiento de cálculos financieros para garantizar mayor precisión en indicadores y reportes",
]

SUMMARY_X = 540
SUMMARY_Y = 440

print("\n")
print("Tienes 5 segundos para enfocar Jira...")
print("\n")

time.sleep(5)

for i, task in enumerate(TASKS, start=1):
    print(f"[{i}/{len(TASKS)}] {task}")

    pyautogui.click(SUMMARY_X, SUMMARY_Y)

    pyautogui.hotkey("command", "a")
    pyautogui.press("backspace")

    pyautogui.write(task, interval=0.01)

    time.sleep(0.4)

    pyautogui.hotkey("command", "enter")

    time.sleep(1.5)

print("Terminado.")
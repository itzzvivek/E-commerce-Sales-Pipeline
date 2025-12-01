def notify_success(task_name, details=None):
    print("Pipline Notification")
    print(f"Task: {task_name}")
    print("Status: Success")

    if details:
        print("Details:", details)
def send_outlook_mail(subject: str, body: str, to: str):
    import win32com.client

    outlook = win32com.client.Dispatch("Outlook.Application")
    mail = outlook.CreateItem(0)

    mail.To = to
    mail.Subject = subject
    mail.Body = body

    mail.Send()

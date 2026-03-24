package services

import (
    "fmt"
    "net/smtp"
    "os"
)

func SendMail(to, subject, body string) error {
    host := os.Getenv("SMTP_HOST")
    port := os.Getenv("SMTP_PORT")
    user := os.Getenv("SMTP_USER")
    pass := os.Getenv("SMTP_PASS")
    from := os.Getenv("SMTP_FROM")

    addr := host + ":" + port
    auth := smtp.PlainAuth("", user, pass, host)

    msg := "From: " + from + "\r\n" +
        "To: " + to + "\r\n" +
        "Subject: " + subject + "\r\n\r\n" +
        body + "\r\n"

    return smtp.SendMail(addr, auth, from, []string{to}, []byte(msg))
}

func BuildPasswordResetEmail(link string) (string, string) {
    subject := "Reset Your Password"
    body := fmt.Sprintf(`
Hello,

We received a request to reset your password.

Click the link below to reset it:

%s

If you did not request this, you can safely ignore this email.

Thanks,
DDM Portal Team
`, link)

    return subject, body
}

class LogEntry:

    # Reference taken from code shared by TA for Assignment 2.1

    def __init__(self, email, name, num_emails, num_words, to, to_mail, subject):
        self.email = email
        self.name = name
        self.num_emails = num_emails
        self.num_words = num_words
        self.to = to
        self.to_mail = to_mail
        self.subject = subject

    def __str__(self):
        return "LogEntry {\n\temail: %s,\n\tname: %s,\n\tnum_emails: %s,\n\tnum_words: %s,\n\tto: %s,\n\tto_mail: %s,\n\tsubject: %s\n}" \
               % (self.email, self.name, self.num_emails, self.num_words, self.to, self.to_mail,self.subject)

    def __repr__(self):
        return "LogEntry {\n\temail: %s,\n\tname: %s,\n\tnum_emails: %s,\n\tnum_words: %s,\n\tto: %s,\n\tto_mail: %s,\n\tsubject: %s\n}" \
               % (self.email, self.name, self.num_emails, self.num_words, self.to, self.to_mail,self.subject)

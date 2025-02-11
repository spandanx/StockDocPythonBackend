import paramiko
import pandas as pd
from datetime import datetime
import os

class SFTPMLPrediction:
    def __init__(self, sftp_host, sftp_username, sftp_password):
        self.sftp_host = sftp_host
        self.sftp_username = sftp_username
        self.sftp_password = sftp_password

    def get_prediction(self, prediction_file_path):
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.sftp_host, username=self.sftp_username, password=self.sftp_password)
            sftp = ssh.open_sftp()

            with sftp.open(prediction_file_path, "r") as f:
                df_read_from_sftp = pd.read_csv(f)
            return df_read_from_sftp.to_json(orient='records')

    def get_csv_files_in_folder(self, folder_path):
        files = []
        with paramiko.SSHClient() as ssh:
            # ssh.load_system_host_keys()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.sftp_host, username=self.sftp_username, password=self.sftp_password)

            sftp = ssh.open_sftp()
            filesInSFTP = sftp.listdir_attr(folder_path)
            filesInSFTP.sort(key=lambda f: -f.st_mtime)
            for file_ in filesInSFTP:
                file_permissions = str(file_).split(' ')[0]
                if 'd' not in file_permissions and file_.filename.endswith(".csv"):
                    files.append(folder_path + '/' + file_.filename)
                    # t = datetime.fromtimestamp(file_.st_mtime).strftime('%Y-%m-%dT%H:%M:%S')
                    # print(file_.filename, file_.st_size, t, str(file_).split(' ')[0])
            # print(filesInSFTP)
            # print(files)
        return files


if __name__ == "__main__":
    pass
    # print(sftpMLPrediction.get_csv_files_in_folder("/home/gamma/predictions/"))
    # print(sftpMLPrediction.get_prediction('/home/gamma/predictions/mlmodel_prediction.csv'))
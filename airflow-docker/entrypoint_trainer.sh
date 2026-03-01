#!/bin/bash
# entrypoint_trainer.sh

echo "----------------------------------------"
echo "Starting SSH Server for Model Training..."
echo "User: root | Password: root123"
echo "----------------------------------------"

# Khởi động SSH Daemon ở chế độ không detach (-D)
# và gửi log ra màn hình (-e) để tiện debug
exec /usr/sbin/sshd -D -e
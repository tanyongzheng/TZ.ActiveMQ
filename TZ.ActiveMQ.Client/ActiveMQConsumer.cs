using System;
using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;
using Microsoft.Extensions.Options;

namespace TZ.ActiveMQ.Client
{
    public class ActiveMQConsumer : ActiveMQClientBase, IDisposable
    {
        public ActiveMQConsumer(IOptions<ActiveMQClientOptions> options) : base(options)
        {
        }
        
        /// <summary>
        /// 打开连接
        /// </summary>
        public void Open()
        {
            if (string.IsNullOrWhiteSpace(this.BrokerUri))
                throw new MemberAccessException("未指定BrokerUri");
            if (string.IsNullOrWhiteSpace(this.QueueName))
                throw new MemberAccessException("未指定QueueName");

            var factory = new ConnectionFactory(this.BrokerUri);
            if (string.IsNullOrWhiteSpace(this.UserName) && string.IsNullOrWhiteSpace(this.Password))
                _connection = factory.CreateConnection();
            else
                _connection = factory.CreateConnection(this.UserName, this.Password);
            _connection.Start();
            _session = _connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

            switch (MQMode)
            {
                case MQMode.Queue:
                    {
                        _consumer = _session.CreateConsumer(new ActiveMQQueue(this.QueueName));
                        break;
                    }
                case MQMode.Topic:
                    {
                        _consumer = _session.CreateConsumer(new ActiveMQTopic(this.QueueName));
                        break;
                    }
                default:
                    {
                        throw new Exception($"无法识别的MQMode类型:{MQMode.ToString()}");
                    }
            }
        }

        /// <summary>
        /// 关闭连接
        /// </summary>
        public void Close()
        {
            _consumer?.Close();
            _session?.Close();
            _connection?.Close();
        }


        /// <summary>
        /// 执行与释放或重置非托管资源相关的应用程序定义的任务。
        /// </summary>
        public void Dispose()
        {
            this.Close();
        }

        #region 接收消息
        /// <summary>
        /// 设置监听接收到消息后的方法-队列消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="action"></param>
        /// <param name="queueName"></param>
        public void SetQueueMessageReceivedAction<T>(Action<T> action, string queueName) where T : class
        {
            SetMessageReceivedAction(action, MQMode.Queue, queueName);
        }

        /// <summary>
        /// 设置监听接收到消息后的方法-订阅消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="action"></param>
        /// <param name="queueName"></param>
        public void SetTopicMessageReceivedAction<T>(Action<T> action, string queueName) where T : class
        {
            SetMessageReceivedAction(action, MQMode.Topic, queueName);
        }

        /// <summary>
        /// 设置监听接收到消息后的方法
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="action"></param>
        /// <param name="mqMode"></param>
        /// <param name="queueName"></param>
        private void SetMessageReceivedAction<T>(Action<T> action, MQMode mqMode, string queueName) where T : class
        {
            #region 消费者
            _consumer.Listener += (msg) =>
            {
                if (msg is ActiveMQTextMessage textMessage)
                {
                    var result = textMessage.Text as T;
                    action(result);
                }
                else if (msg is ActiveMQBytesMessage bytesMessage)
                {
                    var buffer = new byte[bytesMessage.BodyLength];
                    bytesMessage.WriteBytes(buffer);
                    var result = ToObject<T>(buffer);
                    action(result);
                }
                else if (msg is ActiveMQObjectMessage objectMessage)
                {
                    var result = (T)objectMessage.Body;
                    action(result);
                }
            };
            Open();
            #endregion
        } 
        #endregion

    }
}
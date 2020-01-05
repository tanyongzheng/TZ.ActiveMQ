using System;
using System.Collections.Concurrent;
using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;
using Microsoft.Extensions.Options;

namespace TZ.ActiveMQ.Client
{
    public class ActiveMQProducer:ActiveMQClientBase, IDisposable
    {

        public ActiveMQProducer(IOptions<ActiveMQClientOptions> options) : base(options)
        {
        }
        /// <summary>
        /// 队列缓存字典
        /// </summary>
        private readonly ConcurrentDictionary<string, IMessageProducer> _concrtProcuder = new ConcurrentDictionary<string, IMessageProducer>();

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
            //IConnectionFactory factory = new NMSConnectionFactory(this.BrokerUri);
            if (string.IsNullOrWhiteSpace(this.UserName) && string.IsNullOrWhiteSpace(this.Password))
                _connection = factory.CreateConnection();
            else
                _connection = factory.CreateConnection(this.UserName, this.Password);
            _connection.Start();
            _session = _connection.CreateSession();

            CreateProducer(this.QueueName);
        }


        /// <summary>
        /// 关闭连接
        /// </summary>
        public void Close()
        {
            IMessageProducer _p = null;
            foreach (var p in this._concrtProcuder)
            {
                if (this._concrtProcuder.TryGetValue(p.Key, out _p))
                {
                    _p?.Close();
                }
            }
            this._concrtProcuder.Clear();

            _session?.Close();
            _connection?.Close();
        }

        /// <summary>
        /// 向队列发送数据
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="body">数据</param>
        private void Put<T>(T body)
        {
            Send(this.QueueName, body);
        }

        /// <summary>
        /// 向指定队列发送数据
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="body">数据</param>
        /// <param name="queueName">指定队列名</param>
        private void Put<T>(T body, string queueName)
        {
            Send(queueName, body);
        }

        /// <summary>
        /// 创建队列
        /// </summary>
        /// <param name="queueName"></param>
        private IMessageProducer CreateProducer(string queueName)
        {
            if (_session == null)
            {
                Open();
            }

            //创建新生产者
            Func<string, IMessageProducer> CreateNewProducter = (name) =>
            {
                IMessageProducer newProducer = null;
                switch (MQMode)
                {
                    case MQMode.Queue:
                        {
                            newProducer = _session.CreateProducer(new ActiveMQQueue(name));
                            //队列模式，适合一对一的情况
                            /*var destination = SessionUtil.GetDestination(_session, "queue://" + name);
                            _newProducer = _session.CreateProducer(destination);*/
                            break;
                        }
                    case MQMode.Topic:
                        {
                            newProducer = _session.CreateProducer(new ActiveMQTopic(name));
                            //发布/订阅模式，适合一对多的情况
                            /*var destination = SessionUtil.GetDestination(_session, "topic://" + name);
                            _newProducer = _session.CreateProducer(destination);*/
                            break;
                        }
                    default:
                        {
                            throw new Exception($"无法识别的MQMode类型:{MQMode.ToString()}");
                        }
                }
                return newProducer;
            };
            // ConcurrentDictionary使用GetOrAdd方法添加委托的Value存在线程安全问题，可使用Lazy类型来避免
            // https://www.cnblogs.com/CreateMyself/p/6086752.html    
            //多线程情况下ConcurrentDictionary的方法只保证key/value线程安全，不能保证key/valueFactory的线程安全,直接把新建的生产者当做value才行
            var newProducter = CreateNewProducter(queueName);
            return this._concrtProcuder.GetOrAdd(queueName + "-" + MQMode, newProducter);
        }

        /// <summary>
        /// 发送数据
        /// </summary>
        /// <param name="queueName">队列名称</param>
        /// <typeparam name="T"></typeparam>
        /// <param name="body">数据</param>
        private void Send<T>(string queueName, T body)
        {
            var producer = CreateProducer(queueName);
            IMessage msg;
            if (body is byte[])
            {
                msg = producer.CreateBytesMessage(body as byte[]);
            }
            else if (body is string)
            {
                msg = producer.CreateTextMessage(body as string);
            }
            else
            {
                msg = producer.CreateObjectMessage(body);
            }
            if (msg != null)
            {
                producer.Send(msg, MsgDeliveryMode.Persistent, MsgPriority.Normal, TimeSpan.MinValue);
            }
        }

        /// <summary>
        /// 执行与释放或重置非托管资源相关的应用程序定义的任务。
        /// </summary>
        public void Dispose()
        {
            this.Close();
        }


        #region 发送消息

        /// <summary>
        /// 发送队列消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message"></param>
        /// <param name="queueName"></param>
        public void SendQueueMessage<T>(T message, string queueName) where T : class
        {
            SendMessage(message, MQMode.Queue, queueName);
        }

        /// <summary>
        /// 发送订阅消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message"></param>
        /// <param name="queueName"></param>
        public void SendTopicMessage<T>(T message, string queueName) where T : class
        {
            SendMessage(message, MQMode.Topic, queueName);
        }


        private void SendMessage<T>(T message, MQMode mqMode, string queueName) where T : class
        {
            var msgType = message.GetType();
            if (msgType != typeof(string) && msgType != typeof(byte[]))
            {
                var serializableAttrs = msgType.GetCustomAttributes(typeof(SerializableAttribute), false);
                if (serializableAttrs.Length < 1)
                {
                    throw new Exception("message的类型需要添加序列化特性SerializableAttribute（string和byte[]类型除外）");
                }
            }
            #region 生产者
            QueueName = queueName;
            MQMode = mqMode;
            Open();
            //发送到队列, Put对象类必须使用[Serializable]注解属性
            Put(message);
            #endregion
        }
        #endregion
    }
}
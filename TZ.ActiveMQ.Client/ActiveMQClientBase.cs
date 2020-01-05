using Apache.NMS;
using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Extensions.Options;

namespace TZ.ActiveMQ.Client
{
    public class ActiveMQClientBase
    {

        #region 监听连接对象
        protected IConnection _connection;
        protected ISession _session;
        protected IMessageConsumer _consumer;
        #endregion

        protected ActiveMQClientOptions ActiveMqOptions;

        //private string ConnectionString;
        /// <summary>
        /// 构造函数从配置环境中取ActiveMQClientOptions
        /// </summary>
        public ActiveMQClientBase(IOptions<ActiveMQClientOptions> options)
        {
            if (options == null || options.Value == null)
            {
                throw new Exception("please set ActiveMQClientOptions!");
            }
            else if (options.Value != null)
            {
                ActiveMqOptions = options.Value;
            }

            this.BrokerUri = ActiveMqOptions.BrokerUri;
            this.UserName = ActiveMqOptions.UserName;
            this.Password = ActiveMqOptions.Password;
        }

        /// <summary>
        /// 连接地址
        /// </summary>
        public string BrokerUri { get; set; }

        /// <summary>
        /// 用于登录的用户名,必须和密码同时指定
        /// </summary>
        public string UserName { get; set; }

        /// <summary>
        /// 用于登录的密码,必须和用户名同时指定
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// 队列名称
        /// </summary>
        public string QueueName { get; set; }

        /// <summary>
        /// 指定使用队列的模式
        /// </summary>
        public MQMode MQMode { get; set; }


        /// <summary>
        /// 将对象转换为bytes
        /// </summary>
        /// <param name="obj"></param>
        /// <returns>bytes</returns>
        public byte[] ToBytes<T>(T obj) where T : class
        {
            if (obj == null)
                return null;
            using (var ms = new MemoryStream())
            {
                var formatter = new BinaryFormatter();
                formatter.Serialize(ms, obj);
                return ms.GetBuffer();
            }
        }

        /// <summary>
        /// 将bytes转换为对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="bytes"></param>
        /// <returns></returns>
        public T ToObject<T>(byte[] bytes) where T : class
        {
            if (bytes == null)
                return default(T);
            using (var ms = new MemoryStream(bytes))
            {
                var formatter = new BinaryFormatter();
                return formatter.Deserialize(ms) as T;
            }
        }
    }
}
namespace TZ.ActiveMQ.Client
{
    public class ActiveMQClientOptions
    {
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
    }
}
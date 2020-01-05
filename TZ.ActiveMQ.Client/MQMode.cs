namespace TZ.ActiveMQ.Client
{

    /// <summary>
    /// 队列模式
    /// </summary>
    public enum MQMode
    {
        /// <summary>
        /// 队列，点对点模式。
        /// 使用此模式。一个生产者向队列存入一条消息之后,只有一个消费者能触发消息接收事件。
        /// </summary>
        Queue,

        /// <summary>
        /// 主题，发布者/订阅模式。
        /// 使用此模式，一个生产者向队列存入一条消息之后,所有订阅当前的主题的消费者都能触发消息接收事件。
        /// 使用此模式，必须先创建消费者，再创建生产者。
        /// </summary>
        Topic
    }
}
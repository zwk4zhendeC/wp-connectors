这是Doris的sink插件。

他原本使用MYSQL协议编写，请使用Arrow Flight SQL协议重构它。保留自动建表，但是不保留自动识别字段的功能。
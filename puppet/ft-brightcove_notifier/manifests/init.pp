class video_mapper {

  $binary_name = "video-mapper"
  $install_dir = "/usr/local/$binary_name"
  $binary_file = "$install_dir/$binary_name"
  $log_dir = "/var/log/apps"

  $q_addr = hiera('q_addr')
  $q_group = hiera('q_group')
  $q_read_topic = hiera('q_read_topic')
  $q_read_queue = hiera('q_read_queue')
  $q_write_topic = hiera('q_write_topic')
  $q_write_queue = hiera('q_write_queue')
  $q_authorization = hiera('q_authorization')

  class { 'common_pp_up': }
  class { "${module_name}::monitoring": }
  class { "${module_name}::supervisord": }

  Class["${module_name}::supervisord"] -> Class['common_pp_up'] -> Class["${module_name}::monitoring"]

  user { $binary_name:
    ensure    => present,
  }

  file {
    $install_dir:
      mode    => "0664",
      ensure  => directory;

    $binary_file:
      ensure  => present,
      source  => "puppet:///modules/$module_name/$binary_name",
      mode    => "0755",
      require => File[$install_dir];

    $log_dir:
      ensure  => directory,
      mode    => "0664"
  }

  exec { 'restart_app':
    command     => "supervisorctl restart $binary_name",
    path        => "/usr/bin:/usr/sbin:/bin",
    subscribe   => [
      File[$binary_file],
      Class["${module_name}::supervisord"]
    ],
    refreshonly => true
  }
}

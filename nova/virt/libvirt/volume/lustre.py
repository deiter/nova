#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import errno
import os

from oslo_concurrency import lockutils
from oslo_concurrency import processutils
from oslo_log import log as logging

import nova.conf
import nova.privsep.fs
from nova import utils
from nova.virt.libvirt import utils as libvirt_utils
from nova.virt.libvirt.volume import fs
from nova.virt.libvirt.volume import remotefs

CONF = nova.conf.CONF

LOG = logging.getLogger(__name__)


class LibvirtLustreVolumeDriver(fs.LibvirtMountedFileSystemVolumeDriver):
    """Class implements libvirt part of volume driver for Lustre."""

    def __init__(self, connection):
        super(LibvirtLustreVolumeDriver, self).__init__(connection, 'lustre')

    def _get_mount_point_base(self):
        """Return the mount point path prefix.

        This is used to build the device path.

        :returns: The mount point path prefix.
        """
        return CONF.libvirt.lustre_mount_point_base

    def get_config(self, connection_info, disk_info):
        """Returns xml for libvirt."""
        conf = super(LibvirtLustreVolumeDriver,
                     self).get_config(connection_info, disk_info)
        data = connection_info['data']
        conf.source_type = 'file'
        conf.source_path = data['device_path']
        conf.driver_format = data.get('format', 'raw')
        conf.driver_io = "native"
        return conf

    def _mount_options(self, connection_info):
        """Return a list of additional arguments to pass to the mount command.
        """
        options = []
        data = connection_info['data']
        conf_options = CONF.libvirt.lustre_mount_options
        if conf_options:
            options.extend(['-o', conf_options])
        data_options = data.get('options')
        if data_options:
            options.extend(data_options.split())
        return options

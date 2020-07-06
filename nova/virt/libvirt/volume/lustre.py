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

from oslo_concurrency import processutils
from oslo_log import log as logging
from oslo_utils import fileutils
import six

import nova.conf
import nova.privsep.fs
from nova.i18n import _
from nova import utils
from nova.virt.libvirt import utils as libvirt_utils
from nova.virt.libvirt.volume import fs
from nova.virt.libvirt.volume import remotefs

CONF = nova.conf.CONF

LOG = logging.getLogger(__name__)


class LibvirtLustreVolumeDriver(fs.LibvirtBaseFileSystemVolumeDriver):
    """Class implements libvirt part of volume driver for Lustre."""

    def _get_mount_point_base(self):
        return CONF.libvirt.lustre_mount_point_base

    def get_config(self, connection_info, disk_info):
        """Returns xml for libvirt."""
        conf = super(LibvirtLustreVolumeDriver,
                     self).get_config(connection_info, disk_info)
        data = connection_info['data']
        conf.source_type = 'file'
        conf.source_path = data['device_path']
        conf.driver_format = data.get('format', 'raw')
        return conf

    def connect_volume(self, connection_info, instance):
        data = connection_info['data']
        lustre_share = data['export']
        mount_path = self._get_mount_path(connection_info)
        if not libvirt_utils.is_mounted(mount_path, lustre_share):
            options = []
            conf_options = CONF.libvirt.lustre_mount_options
            if conf_options:
                options.extend(['-o', conf_options])
            data_options = data.get('options')
            if data_options:
                options.extend(data_options.split())
            try:
                remotefs.mount_share(mount_path, lustre_share,
                                     'lustre', options=options)
            except processutils.ProcessExecutionError as exp:
                if exp.exit_code == errno.EEXIST:
                    LOG.warning(exp.stderr)
                else:
                    raise
        device_path = self._get_device_path(connection_info)
        data['device_path'] = device_path

    def disconnect_volume(self, connection_info, instance):
        """Disconnect the volume."""

        lustre_share = connection_info['data']['export']
        mount_path = self._get_mount_path(connection_info)
        remotefs.unmount_share(mount_path, lustre_share)

    def _ensure_mounted(self, connection_info):
        """@type connection_info: dict
        """
        lustre_export = connection_info['data']['export']
        mount_path = self._get_mount_path(connection_info)
        if not libvirt_utils.is_mounted(mount_path, lustre_export):
            options = connection_info['data'].get('options')
            self._mount_lustre(mount_path, lustre_export,
                                  options, ensure=True)
        return mount_path

    def _mount_lustre(self, mount_path, lustre_share,
                         options=None, ensure=False):
        """Mount lustre export to mount path."""
        fileutils.ensure_tree(mount_path)

        lustre_cmd = ['mount', '-t', 'lustre']
        if options is not None:
            lustre_cmd.extend(options.split(' '))
        lustre_cmd.extend([lustre_share, mount_path])

        try:
            utils.execute(*lustre_cmd, run_as_root=True)
            #nova.privsep.fs.mount(export_type, export_path, mount_path, options)
        except processutils.ProcessExecutionError as exc:
            if ensure and 'already mounted' in six.text_type(exc):
                LOG.warning(_("Lustre share %s is already mounted"),
                            lustre_share)
            else:
                raise

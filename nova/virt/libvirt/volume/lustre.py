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


from oslo_concurrency import processutils
from oslo_log import log as logging
import six

import nova.conf
from nova.i18n import _
from nova import utils
from nova.virt.libvirt import utils as libvirt_utils
from nova.virt.libvirt.volume import fs

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

        if 'lustre' in CONF.libvirt.qemu_allowed_storage_drivers:
            vol_name = data['export'].split('/')[1]
            source_host = data['export'].split('/')[0][:-1]

            conf.source_ports = ['24007']
            conf.source_type = 'network'
            conf.source_protocol = 'lustre'
            conf.source_hosts = [source_host]
            conf.source_name = '%s/%s' % (vol_name, data['name'])
        else:
            conf.source_type = 'file'
            conf.source_path = connection_info['data']['device_path']

        conf.driver_format = connection_info['data'].get('format', 'raw')

        return conf

    def connect_volume(self, connection_info, instance):
        if 'lustre' not in CONF.libvirt.qemu_allowed_storage_drivers:
            self._ensure_mounted(connection_info)
            connection_info['data']['device_path'] = \
                self._get_device_path(connection_info)

    def disconnect_volume(self, connection_info, disk_dev, instance):
        """Disconnect the volume."""

        if 'lustre' in CONF.libvirt.qemu_allowed_storage_drivers:
            return

        mount_path = self._get_mount_path(connection_info)

        try:
            utils.execute('umount', mount_path, run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            export = connection_info['data']['export']
            if 'target is busy' in six.text_type(exc):
                LOG.debug("The Lustre share %s is still in use.", export)
            else:
                LOG.exception(_("Couldn't unmount the Lustre share %s"),
                              export)

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
        utils.execute('mkdir', '-p', mount_path)

        lustre_cmd = ['mount', '-t', 'lustre']
        if options is not None:
            lustre_cmd.extend(options.split(' '))
        lustre_cmd.extend([lustre_share, mount_path])

        try:
            utils.execute(*lustre_cmd, run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            if ensure and 'already mounted' in six.text_type(exc):
                LOG.warning(_("Lustre share %s is already mounted"),
                            lustre_share)
            else:
                raise

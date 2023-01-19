# Copyright 2013-2022 Lawrence Livermore National Security, LLC and other
# Spack Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: (Apache-2.0 OR MIT)

# ----------------------------------------------------------------------------
# If you submit this package back to Spack as a pull request,
# please first remove this boilerplate and all FIXME comments.
#
# This is a template package file for Spack.  We've put "FIXME"
# next to all the things you'll want to change. Once you've handled
# them, you can save this file and test your package like this:
#
#     spack install imss
#
# You can edit this file again by typing:
#
#     spack edit imss
#
# See the Spack documentation for more information on packaging.
# ----------------------------------------------------------------------------

from spack import *


class Imss(CMakePackage):
    """FIXME: Put a proper description of your package here."""

    # FIXME: Add a proper url for your package's homepage here.
    homepage = "https://www.example.com"
    url      = "https://gitlab.arcos.inf.uc3m.es/admire/imss/-/raw/master/releases/download/v1.0/imss.tgz"
    git      = "https://gitlab.arcos.inf.uc3m.es/admire/imss.git"

    # FIXME: Add a list of GitHub accounts to
    # notify when the package is updated.
    # maintainers = ['github_user1', 'github_user2']
    maintainers = ['fjblas']

    version('1.0', sha256='14f45e5e43176dd1ffbcbed7674089d56192ca5e43675f4055aaffa6ee3d26e9')

    # FIXME: Add dependencies if required.
    depends_on('glib')
    depends_on('pcre')
    depends_on('ucx mlx5_dv=True openmp=True verbs=True dm=True rc=True')
    depends_on('cmake', type='build')
    depends_on('glib')

    def cmake_args(self):
        cmake_args = [
            '-DCMAKE_INSTALL_SYSCONFDIR={0}'.format(self.spec.prefix.etc),
            '-DCMAKE_INSTALL_RUNDIR=/var/run'
        ]

        if self.spec.satisfies('@:39.0'):
            cmake_args.extend([
                self.define('PYTHON_LIBRARY', self.spec['python'].libs[0]),
                self.define('PYTHON_INCLUDE_DIR',
                            self.spec['python'].headers.directories[0])
            ])
        return cmake_args
~                           

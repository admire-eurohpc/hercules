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


class Hercules(CMakePackage):
    """Hercules, a network portable ad-hoc file system"""

    # FIXME: Add a proper url for your package's homepage here.
    homepage = "https://gitlab.arcos.inf.uc3m.es/admire/imss.git"
    url      = "https://gitlab.arcos.inf.uc3m.es/admire/imss/-/raw/master/releases/download/v1.0/imss.tgz"
    git      = "https://gitlab.arcos.inf.uc3m.es/admire/imss.git"

    # FIXME: Add a list of GitHub accounts to
    # notify when the package is updated.
    # maintainers = ['github_user1', 'github_user2']
    maintainers = ['fjblas']

    version('latest', branch='master')
    version('1.0', sha256='996ad688508bbbba42c3bc69ea6352834bf3e83708e42397dd4c28b6cff888a7')

    # FIXME: Add dependencies if required.
    depends_on('glib')
    depends_on('pcre')
    depends_on('jemalloc')
    depends_on('ucx mlx5_dv=True openmp=True verbs=True dm=True rc=True thread_multiple=True ucg=True xpmem=True dc=True rdmacm=True knem=True cma=True ib_hw_tm=True rocm=True ud=True')
    depends_on('cmake', type='build')

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

///<reference path="../../headers/common.d.ts" />

export default class AdminListUsersCtrl {

  users: any;
  pages = [];
  perPage = 1;
  page = 1;
  totalPages: number;

  /** @ngInject */
  constructor(private $scope, private backendSrv) {
    this.getUsers();
  }

  getUsers() {
    this.backendSrv.get(`/api/users/search?perpage=${this.perPage}&page=${this.page}`).then((result) => {
      this.users = result.users;

      this.page = result.page;
      this.perPage = result.perPage;
      this.totalPages = result.totalCount / result.perPage;
      this.pages = [];
      for (var i = 1; i < this.totalPages+1; i++) {
        this.pages.push({ page: i, current: i === this.page});
      }
    });
  }

  navigateToPage(page) {
    this.page = page.page;
    this.getUsers();
  }

  deleteUser(user) {
    this.$scope.appEvent('confirm-modal', {
      title: 'Delete',
      text: 'Do you want to delete ' + user.login + '?',
      icon: 'fa-trash',
      yesText: 'Delete',
      onConfirm: () => {
        this.backendSrv.delete('/api/admin/users/' + user.id).then(() => {
          this.getUsers();
        });
      }
    });
  }
}
